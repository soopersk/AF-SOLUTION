package com.orchestration.ems.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.orchestration.ems.api.support.AbstractWebMvcTest;
import com.orchestration.ems.config.SecurityConfig;
import com.orchestration.ems.model.GateGroups;
import com.orchestration.ems.store.GateGroupsRepository;

/**
 * Contract slice for {@link GateGroupsController} (ems-design §4.5). The repository is mocked, so this proves
 * the request contract — required {@code group_by}/{@code lookback} (→400), lookback parsing, criteria and
 * json-path pass-through — and the byte-exact response schema {@code {"groups":[{"group","contributors"}]}}.
 * The grouping over real rows is proven in {@code GateGroupsIT}.
 */
@WebMvcTest(GateGroupsController.class)
@Import(SecurityConfig.class)
@WithMockUser(username = "airflow-sensor")
class GateGroupsControllerTest extends AbstractWebMvcTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private GateGroupsRepository repository;

    @Test
    void missingGroupBy_isBadRequest() throws Exception {
        mvc.perform(get("/gate/groups").param("lookback", "5d")).andExpect(status().isBadRequest());
    }

    @Test
    void blankGroupBy_isBadRequest() throws Exception {
        mvc.perform(get("/gate/groups").param("group_by", "").param("lookback", "5d"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void missingLookback_isBadRequest() throws Exception {
        mvc.perform(get("/gate/groups").param("group_by", "context.data.reporting-date"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void malformedLookback_isBadRequest() throws Exception {
        mvc.perform(get("/gate/groups")
                .param("group_by", "context.data.reporting-date")
                .param("lookback", "5weeks")).andExpect(status().isBadRequest());
    }

    @Test
    void groups_is200_withExactWireSchema() throws Exception {
        when(repository.groups(any(), any(), any(), any())).thenReturn(new GateGroups(List.of(
                new GateGroups.Group("2026-07-17", List.of("AMER", "ASIA")),
                new GateGroups.Group("2026-07-18", List.of()))));

        String body = mvc.perform(get("/gate/groups")
                .param("group_by", "context.data.reporting-date")
                .param("lookback", "5d"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals(
                "{\"groups\":[{\"group\":\"2026-07-17\",\"contributors\":[\"AMER\",\"ASIA\"]},"
                + "{\"group\":\"2026-07-18\",\"contributors\":[]}]}",
                body);
    }

    @Test
    void emptyWindow_is200_emptyGroups() throws Exception {
        when(repository.groups(any(), any(), any(), any())).thenReturn(new GateGroups(List.of()));

        mvc.perform(get("/gate/groups")
                .param("group_by", "context.data.reporting-date")
                .param("lookback", "5d"))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Test
    void passesPathsLookbackAndCriteriaThrough() throws Exception {
        when(repository.groups(any(), any(), any(), any())).thenReturn(new GateGroups(List.of()));

        mvc.perform(get("/gate/groups")
                .param("group_by", "context.data.reporting-date")
                .param("contributor", "context.data.companyCode")
                .param("lookback", "6h")
                .param("state", "FINISH|FAILED")
                .param("event_type", "CALC_EVENT")).andExpect(status().isOk());

        ArgumentCaptor<String> groupBy = ArgumentCaptor.forClass(String.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Optional<String>> contributor = ArgumentCaptor.forClass(Optional.class);
        ArgumentCaptor<Duration> lookback = ArgumentCaptor.forClass(Duration.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, List<String>>> criteria = ArgumentCaptor.forClass(Map.class);
        verify(repository).groups(groupBy.capture(), contributor.capture(), lookback.capture(),
                criteria.capture());

        assertThat(groupBy.getValue()).isEqualTo("context.data.reporting-date");
        assertThat(contributor.getValue()).contains("context.data.companyCode");
        assertThat(lookback.getValue()).isEqualTo(Duration.ofHours(6));
        // reserved params are stripped; criteria keep the |-split alternatives
        assertThat(criteria.getValue())
                .containsEntry("state", List.of("FINISH", "FAILED"))
                .containsEntry("event_type", List.of("CALC_EVENT"))
                .doesNotContainKeys("group_by", "contributor", "lookback");
    }

    @Test
    void contributorOptional_absentPassesEmpty() throws Exception {
        when(repository.groups(eq("context.data.reporting-date"), eq(Optional.empty()), any(), any()))
                .thenReturn(new GateGroups(List.of()));

        mvc.perform(get("/gate/groups")
                .param("group_by", "context.data.reporting-date")
                .param("lookback", "5d")).andExpect(status().isOk());

        verify(repository).groups(eq("context.data.reporting-date"), eq(Optional.empty()), any(), any());
    }
}
