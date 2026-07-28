package com.orchestration.ems.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.orchestration.ems.api.support.AbstractWebMvcTest;
import com.orchestration.ems.config.SecurityConfig;
import com.orchestration.ems.model.EnrichedEventView;
import com.orchestration.ems.store.EventQueryRepository;

/**
 * Contract slice for {@link EventController} (ems-design §4.3; legacy old-ems/EventController.scala:37-54).
 * The repository is mocked, so this proves the controller's <b>param handling and status mapping</b> — the
 * real 4-location OR / byte-compat query round-trip is proven in {@code EventQueryRepositoryIT}.
 */
@WebMvcTest(EventController.class)
@Import(SecurityConfig.class)
@WithMockUser(username = "airflow-sensor")
class EventControllerTest extends AbstractWebMvcTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private EventQueryRepository repository;

    @Captor
    private ArgumentCaptor<Optional<String>> eventIdCaptor;
    @Captor
    private ArgumentCaptor<Optional<String>> contextIdCaptor;
    @Captor
    private ArgumentCaptor<Optional<String>> parentIdCaptor;
    @Captor
    private ArgumentCaptor<Map<String, List<String>>> dataParamsCaptor;

    @Test
    void noParams_isBadRequest() throws Exception {
        mvc.perform(get("/event")).andExpect(status().isBadRequest());
    }

    @Test
    void noMatch_is404() throws Exception {
        when(repository.findEvents(any(), any(), any(), any())).thenReturn(List.of());
        mvc.perform(get("/event").param("task_id", "nope")).andExpect(status().isNotFound());
    }

    @Test
    void match_is200_withEnrichedEventListBody() throws Exception {
        when(repository.findEvents(any(), any(), any(), any()))
                .thenReturn(List.of(view("{\"id\":\"evt-mer-1\"}", "{\"id\":\"ctx-300\"}")));

        String body = mvc.perform(get("/event").param("context_id", "ctx-300"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andReturn().getResponse().getContentAsString();

        assertJsonEquals("[{\"event\":{\"id\":\"evt-mer-1\"},\"context\":{\"id\":\"ctx-300\"}}]", body);
    }

    @Test
    void splitsReservedIdsFromDataParams_andPipeSplitsValues() throws Exception {
        when(repository.findEvents(any(), any(), any(), any()))
                .thenReturn(List.of(view("{\"id\":\"e\"}", "{\"id\":\"c\"}")));

        mvc.perform(get("/event")
                .param("event_id", "evt-1")
                .param("context_id", "ctx-1")
                .param("parent_id", "par-1")
                .param("state", "FINISH|FAILED")
                .param("source", "merival"))
                .andExpect(status().isOk());

        org.mockito.Mockito.verify(repository).findEvents(
                eventIdCaptor.capture(), contextIdCaptor.capture(),
                parentIdCaptor.capture(), dataParamsCaptor.capture());

        assertThat(eventIdCaptor.getValue()).contains("evt-1");
        assertThat(contextIdCaptor.getValue()).contains("ctx-1");
        assertThat(parentIdCaptor.getValue()).contains("par-1");
        assertThat(dataParamsCaptor.getValue())
                .containsEntry("state", List.of("FINISH", "FAILED"))
                .containsEntry("source", List.of("merival"))
                .doesNotContainKeys("event_id", "context_id", "parent_id");
    }

    private static EnrichedEventView view(String event, String context) throws JsonProcessingException {
        return new EnrichedEventView(MAPPER.readTree(event), MAPPER.readTree(context));
    }
}
