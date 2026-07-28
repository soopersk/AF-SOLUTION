package com.orchestration.ems.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import com.orchestration.ems.model.RunStatus;
import com.orchestration.ems.store.RunStatusRepository;

/**
 * Contract slice for {@link RunStatusController} (ems-design §4.5). The repository is mocked, so this proves
 * the wire schema (byte-exact field names the framework's {@code SlaAwareHttpTrigger} reads), the
 * always-200 / 400 rules, and correlation-key pass-through. The lifecycle derivation over real rows is proven
 * in {@code RunStatusIT}.
 */
@WebMvcTest(RunStatusController.class)
@Import(SecurityConfig.class)
@WithMockUser(username = "airflow-sensor")
class RunStatusControllerTest extends AbstractWebMvcTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private RunStatusRepository repository;

    @Test
    void noCorrelationKey_isBadRequest() throws Exception {
        mvc.perform(get("/run/status")).andExpect(status().isBadRequest());
    }

    @Test
    void blankCorrelationKey_isBadRequest() throws Exception {
        mvc.perform(get("/run/status").param("context_id", "")).andExpect(status().isBadRequest());
    }

    @Test
    void neverStarted_is200_startedFalse_terminalAbsent() throws Exception {
        when(repository.summarize(Optional.of("ctx-1"), Optional.empty()))
                .thenReturn(new RunStatus(false, false, RunStatus.Terminal.absent(), false, null));

        mvc.perform(get("/run/status").param("context_id", "ctx-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.scheduled").value(false))
                .andExpect(jsonPath("$.started").value(false))
                .andExpect(jsonPath("$.terminal.present").value(false))
                .andExpect(jsonPath("$.terminal.successful").value(false))
                .andExpect(jsonPath("$.terminal.event_id").value(org.hamcrest.Matchers.nullValue()))
                .andExpect(jsonPath("$.dlq_hint").value(false))
                .andExpect(jsonPath("$.last_event_at").value(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    void terminalSuccess_is200_withExactWireSchema() throws Exception {
        when(repository.summarize(Optional.of("ctx-1"), Optional.empty()))
                .thenReturn(new RunStatus(true, true,
                        new RunStatus.Terminal(true, true, "evt-fin-1"), false, "2026-07-26T10:15:30Z"));

        String body = mvc.perform(get("/run/status").param("context_id", "ctx-1"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals(
                "{\"scheduled\":true,\"started\":true,"
                + "\"terminal\":{\"present\":true,\"successful\":true,\"event_id\":\"evt-fin-1\"},"
                + "\"dlq_hint\":false,\"last_event_at\":\"2026-07-26T10:15:30Z\"}",
                body);
    }

    @Test
    void terminalInDlq_is200_dlqHintTrue() throws Exception {
        when(repository.summarize(Optional.of("ctx-1"), Optional.empty()))
                .thenReturn(new RunStatus(true, true, RunStatus.Terminal.absent(), true, "2026-07-26T10:15:30Z"));

        mvc.perform(get("/run/status").param("context_id", "ctx-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.started").value(true))
                .andExpect(jsonPath("$.terminal.present").value(false))
                .andExpect(jsonPath("$.dlq_hint").value(true));
    }

    @Test
    void passesBothCorrelationKeysThrough() throws Exception {
        when(repository.summarize(any(), any()))
                .thenReturn(new RunStatus(true, true, RunStatus.Terminal.absent(), false, null));

        mvc.perform(get("/run/status")
                .param("context_id", "ctx-1")
                .param("task_id", "task-9")).andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Optional<String>> context = ArgumentCaptor.forClass(Optional.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Optional<String>> task = ArgumentCaptor.forClass(Optional.class);
        verify(repository).summarize(context.capture(), task.capture());
        assertThat(context.getValue()).contains("ctx-1");
        assertThat(task.getValue()).contains("task-9");
    }
}
