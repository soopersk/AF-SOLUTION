package com.orchestration.ems.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.orchestration.ems.api.support.AbstractWebMvcTest;
import com.orchestration.ems.config.SecurityConfig;
import com.orchestration.ems.store.ContextQueryRepository;

/**
 * Contract slice for {@link ContextController} (ems-design §4.3; legacy old-ems/EventController.scala:63-112).
 * The repository is mocked, so this proves status mapping + required-param handling; the recursive chain-walk
 * semantics are proven in {@code ContextQueryIT}.
 */
@WebMvcTest(ContextController.class)
@Import(SecurityConfig.class)
@WithMockUser(username = "airflow-sensor")
class ContextControllerTest extends AbstractWebMvcTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private ContextQueryRepository repository;

    @Test
    void context_found_is200_body() throws Exception {
        when(repository.findContextById("ctx-1")).thenReturn(Optional.of(MAPPER.readTree("{\"id\":\"ctx-1\"}")));
        String body = mvc.perform(get("/context").param("context_id", "ctx-1"))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertJsonEquals("{\"id\":\"ctx-1\"}", body);
    }

    @Test
    void context_notFound_is404() throws Exception {
        when(repository.findContextById("nope")).thenReturn(Optional.empty());
        mvc.perform(get("/context").param("context_id", "nope")).andExpect(status().isNotFound());
    }

    @Test
    void context_emptyId_is400() throws Exception {
        mvc.perform(get("/context").param("context_id", "")).andExpect(status().isBadRequest());
    }

    @Test
    void context_missingParam_is400() throws Exception {
        mvc.perform(get("/context")).andExpect(status().isBadRequest());
    }

    @Test
    void parentContext_missingInitial_is400() throws Exception {
        mvc.perform(get("/parentcontext").param("foo", "x")).andExpect(status().isBadRequest());
    }

    @Test
    void parentContext_delegatesWithoutInitialInParams() throws Exception {
        when(repository.findParentContext(eq("ctx-A"), any())).thenReturn(Optional.of(node("{\"id\":\"ctx-B\"}")));

        mvc.perform(get("/parentcontext")
                .param("initial_context_id", "ctx-A")
                .param("foo", "x")).andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> params = ArgumentCaptor.forClass(Map.class);
        verify(repository).findParentContext(eq("ctx-A"), params.capture());
        assertThat(params.getValue()).containsEntry("foo", "x").doesNotContainKey("initial_context_id");
    }

    @Test
    void childContext_missingInitial_is400() throws Exception {
        mvc.perform(get("/childcontext").param("foo", "x")).andExpect(status().isBadRequest());
    }

    @Test
    void childContext_defaultsLimitToOne_andStripsLimitFromParams() throws Exception {
        when(repository.findChildContext(eq("ctx-A"), any(), anyInt())).thenReturn(Optional.of(node("{\"id\":\"c\"}")));

        mvc.perform(get("/childcontext")
                .param("initial_context_id", "ctx-A")
                .param("foo", "x")).andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> params = ArgumentCaptor.forClass(Map.class);
        verify(repository).findChildContext(eq("ctx-A"), params.capture(), eq(1));
        assertThat(params.getValue()).containsEntry("foo", "x")
                .doesNotContainKeys("initial_context_id", "limit");
    }

    @Test
    void childContext_honoursExplicitLimit() throws Exception {
        when(repository.findChildContext(eq("ctx-A"), any(), anyInt())).thenReturn(Optional.empty());

        mvc.perform(get("/childcontext")
                .param("initial_context_id", "ctx-A")
                .param("limit", "5")).andExpect(status().isNotFound());

        verify(repository).findChildContext(eq("ctx-A"), any(), eq(5));
    }

    private static com.fasterxml.jackson.databind.JsonNode node(String json) throws JsonProcessingException {
        return MAPPER.readTree(json);
    }
}
