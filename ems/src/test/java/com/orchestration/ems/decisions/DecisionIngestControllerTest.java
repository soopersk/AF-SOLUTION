package com.orchestration.ems.decisions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.orchestration.ems.api.support.AbstractWebMvcTest;
import com.orchestration.ems.config.GroupAuthorities;
import com.orchestration.ems.config.SecurityConfig;
import com.orchestration.ems.model.DecisionRecord;

/**
 * Contract slice for {@link DecisionIngestController} (ems-design §4.5:219). The repository is mocked, so
 * this proves the request contract — the {@code {"decisions"}} envelope, whole-batch rejection of
 * malformed records, and the V4 tier/decision vocabularies — plus the byte-exact response
 * {@code {"received","inserted"}}. Real persistence is proven in {@code DecisionIngestIT}.
 *
 * <p>The real {@link SecurityConfig} is imported and every request runs as a dispatcher, because
 * {@code decided_by} is now the authenticated principal: with the filter chain stubbed out these tests
 * would assert a contract that no caller can actually reach. Who is refused (and with which status) is
 * the {@code SecurityMatrixTest}'s job.
 */
@WebMvcTest(DecisionIngestController.class)
@Import(SecurityConfig.class)
@WithMockUser(username = "capital_control_dag", authorities = GroupAuthorities.DISPATCHER)
class DecisionIngestControllerTest extends AbstractWebMvcTest {

    /** A well-formed L1_OUTCOME record — the shape a control DAG posts after a TRIGGERED verdict. */
    private static final String TRIGGERED = """
            {"event_id":"evt-9f21","tenant_id":"CAPITAL","tier":"L1_OUTCOME",
             "target_dag_id":"amer_d_b3f_dag","decision":"TRIGGERED",
             "detail":{"dag_run_id":"ab12ef34"},
             "registry_version":"v42/ab99f0","engine_version":"celpy==0.1.5"}
            """;

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private RoutingDecisionRepo repository;

    @Test
    void validBatch_is200_withExactWireSchema() throws Exception {
        when(repository.insertBatch(any(), anyString())).thenReturn(2);

        String body = perform(batch(TRIGGERED, summary()))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("{\"received\":2,\"inserted\":2}", body);
    }

    @Test
    void emptyBatch_is200_zeroCounts() throws Exception {
        String body = perform("{\"decisions\":[]}")
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("{\"received\":0,\"inserted\":0}", body);
    }

    @Test
    void l0Duplicate_absorbed_reportsFewerInserted() throws Exception {
        // ux_rd_l0 swallowed one of the two posted rows — a retry outcome, not a failure
        when(repository.insertBatch(any(), anyString())).thenReturn(1);

        String body = perform(batch(TRIGGERED, summary()))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("{\"received\":2,\"inserted\":1}", body);
    }

    @Test
    void mapsRecordFields_andAuditsTheAuthenticatedPrincipal() throws Exception {
        when(repository.insertBatch(any(), anyString())).thenReturn(1);

        perform(batch(TRIGGERED)).andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<DecisionRecord>> records = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<String> decidedBy = ArgumentCaptor.forClass(String.class);
        verify(repository).insertBatch(records.capture(), decidedBy.capture());

        assertThat(decidedBy.getValue()).isEqualTo("capital_control_dag"); // the @WithMockUser principal
        DecisionRecord r = List.copyOf(records.getValue()).get(0);
        assertThat(r.eventId()).isEqualTo("evt-9f21");
        assertThat(r.tenantId()).isEqualTo("CAPITAL");
        assertThat(r.tier()).isEqualTo("L1_OUTCOME");
        assertThat(r.targetDagId()).isEqualTo("amer_d_b3f_dag");
        assertThat(r.decision()).isEqualTo("TRIGGERED");
        assertThat(r.registryVersion()).isEqualTo("v42/ab99f0");
        assertThat(r.engineVersion()).isEqualTo("celpy==0.1.5");
        assertThat(r.detailJson()).isEqualTo("{\"dag_run_id\":\"ab12ef34\"}");
    }

    @Test
    void decidedByInTheBody_isIgnored_thePrincipalIsAudited() throws Exception {
        // the Python client still sends decided_by; accepting it unchanged keeps that client working,
        // while the value that reaches the audit column stays the one the caller actually proved
        when(repository.insertBatch(any(), anyString())).thenReturn(1);

        perform("{\"decided_by\":\"someone_elses_dag\",\"decisions\":[" + TRIGGERED + "]}")
                .andExpect(status().isOk());

        ArgumentCaptor<String> decidedBy = ArgumentCaptor.forClass(String.class);
        verify(repository).insertBatch(any(), decidedBy.capture());
        assertThat(decidedBy.getValue()).isEqualTo("capital_control_dag");
    }

    @Test
    void missingDecisionsArray_isBadRequest() throws Exception {
        perform("{}").andExpect(status().isBadRequest());
        verifyNoInteractions(repository);
    }

    @Test
    void missingEventId_rejectsWholeBatch() throws Exception {
        String noEventId = """
                {"tier":"L1_SUMMARY","decision":"MATCHED"}
                """;

        perform(batch(TRIGGERED, noEventId)).andExpect(status().isBadRequest());

        verifyNoInteractions(repository); // the valid sibling record is not written either
    }

    @Test
    void unknownTier_isBadRequest() throws Exception {
        perform(batch("{\"event_id\":\"evt-1\",\"tier\":\"L2_MADE_UP\",\"decision\":\"MATCHED\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void unknownDecision_isBadRequest() throws Exception {
        perform(batch("{\"event_id\":\"evt-1\",\"tier\":\"GATE\",\"decision\":\"MAYBE\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void nonObjectDetail_isBadRequest() throws Exception {
        perform(batch(
                "{\"event_id\":\"evt-1\",\"tier\":\"GATE\",\"decision\":\"GATE_OPEN\",\"detail\":\"nope\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void malformedJson_isBadRequest() throws Exception {
        perform("{\"decisions\":").andExpect(status().isBadRequest());
    }

    // --- helpers ------------------------------------------------------------------------------------

    private org.springframework.test.web.servlet.ResultActions perform(String body) throws Exception {
        return mvc.perform(post("/decisions").contentType(MediaType.APPLICATION_JSON).content(body));
    }

    private static String batch(String... records) {
        return "{\"decisions\":[" + String.join(",", records) + "]}";
    }

    /** The one-per-evaluation L1_SUMMARY row — {@code target_dag_id} is null by contract (V4:335). */
    private static String summary() {
        return """
                {"event_id":"evt-9f21","tenant_id":"CAPITAL","tier":"L1_SUMMARY","decision":"MATCHED",
                 "detail":{"matched":1,"errors":0},
                 "registry_version":"v42/ab99f0","engine_version":"celpy==0.1.5"}
                """;
    }
}
