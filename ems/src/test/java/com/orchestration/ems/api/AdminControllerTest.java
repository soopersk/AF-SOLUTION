package com.orchestration.ems.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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
import org.springframework.test.web.servlet.ResultActions;

import com.orchestration.ems.api.support.AbstractWebMvcTest;
import com.orchestration.ems.config.GroupAuthorities;
import com.orchestration.ems.config.SecurityConfig;
import com.orchestration.ems.ingestion.DlqReplayService;
import com.orchestration.ems.model.DlqReplay;
import com.orchestration.ems.model.DlqReplay.Reason;
import com.orchestration.ems.model.DlqReplay.Skipped;
import com.orchestration.ems.model.SubscriptionRow;
import com.orchestration.ems.model.SubscriptionRow.Stage;
import com.orchestration.ems.subscription.CelPrograms;
import com.orchestration.ems.subscription.SubscriptionRepo;

/**
 * Contract slice for {@link AdminController} (ems-design §4.5:220-221). The replay engine and the
 * subscription repository are mocked, so this proves the request/response contracts and the write-path
 * validation — including the two rejections §4.5:221 names explicitly.
 *
 * <p>{@link CelPrograms} is deliberately the <b>real</b> component rather than a mock: "rejects CEL that
 * fails compilation" and the A4 {@code PERSIST}-may-not-see-{@code context} rule are properties of the
 * compiler, so a stubbed one would assert nothing. Persistence is proven in {@code SubscriptionUpsertIT}
 * and {@code DlqReplayIT}.
 *
 * <p>The real {@link SecurityConfig} is imported and every request runs as the registry-CI principal
 * (elevated <b>and</b> CI, as {@code PUT /admin/subscriptions} requires), because {@code updated_by} and
 * {@code replayed_by} are now taken from it. Who is <em>refused</em> is {@code SecurityMatrixTest}'s job.
 */
@WebMvcTest(AdminController.class)
@Import({ CelPrograms.class, SecurityConfig.class })
@WithMockUser(username = AdminControllerTest.CI,
        authorities = { GroupAuthorities.ADMIN, GroupAuthorities.CI })
class AdminControllerTest extends AbstractWebMvcTest {

    /** The authenticated principal for every request here — and therefore the audited identity. */
    static final String CI = "registry-ci";

    /** A well-formed FORWARD row — the shape the registry CI renders (ems-design §4.5:221). */
    private static final String FORWARD_ROW = """
            {"tenant":"CAPITAL","stage":"FORWARD","rule_name":"cap_data_update.MER_batch",
             "control_dag_id":"orchestration_control_dag_capital",
             "when":"event.additionalData.tenant == \\"FRCA\\" && context.data.status == \\"COMPLETE\\"",
             "registry_version":"v42/ab99f0"}
            """;

    /** A well-formed PERSIST row — event-only CEL, no control DAG (A4). */
    private static final String PERSIST_ROW = """
            {"tenant":"PLATFORM","stage":"PERSIST","rule_name":"persist_frca",
             "when":"event.additionalData.tenant == \\"FRCA\\"",
             "registry_version":"v42/ab99f0"}
            """;

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private DlqReplayService replayService;

    @MockitoBean
    private SubscriptionRepo subscriptions;

    // --- POST /admin/replay -------------------------------------------------------------------------

    @Test
    void replay_is200_withExactWireSchema() throws Exception {
        when(replayService.replay(any(), anyString())).thenReturn(new DlqReplay.Result(
                2, 1, List.of(new Skipped(7L, Reason.ALREADY_REPLAYED))));

        String body = replay("{\"ids\":[6,7]}")
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("""
                {"requested":2,"replayed":1,"skipped":[{"id":7,"reason":"ALREADY_REPLAYED"}]}""", body);
    }

    @Test
    void replay_passesIds_andAuditsTheAuthenticatedOperator() throws Exception {
        when(replayService.replay(any(), anyString())).thenReturn(new DlqReplay.Result(2, 2, List.of()));

        replay("{\"ids\":[6,7]}").andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Long>> ids = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<String> replayedBy = ArgumentCaptor.forClass(String.class);
        verify(replayService).replay(ids.capture(), replayedBy.capture());

        assertThat(ids.getValue()).containsExactly(6L, 7L);
        assertThat(replayedBy.getValue()).isEqualTo(CI); // the principal, not a body field
    }

    @Test
    void replay_emptySelection_is200_zeroCounts() throws Exception {
        when(replayService.replay(any(), anyString())).thenReturn(new DlqReplay.Result(0, 0, List.of()));

        String body = replay("{\"ids\":[]}")
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("{\"requested\":0,\"replayed\":0,\"skipped\":[]}", body);
    }

    @Test
    void replay_missingIds_isBadRequest() throws Exception {
        replay("{}").andExpect(status().isBadRequest());
        verifyNoInteractions(replayService);
    }

    // --- PUT /admin/subscriptions -------------------------------------------------------------------

    @Test
    void upsert_is200_withExactWireSchema() throws Exception {
        when(subscriptions.upsertAll(any(), anyString())).thenReturn(2);

        String body = upsert(slice(FORWARD_ROW, PERSIST_ROW))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("{\"received\":2,\"upserted\":2}", body);
    }

    @Test
    void upsert_mapsDesignWireNamesOntoTheV3Row() throws Exception {
        when(subscriptions.upsertAll(any(), anyString())).thenReturn(1);

        upsert(slice(FORWARD_ROW)).andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SubscriptionRow>> rows = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<String> updatedBy = ArgumentCaptor.forClass(String.class);
        verify(subscriptions).upsertAll(rows.capture(), updatedBy.capture());

        assertThat(updatedBy.getValue()).isEqualTo(CI);
        SubscriptionRow row = List.copyOf(rows.getValue()).get(0);
        assertThat(row.tenantId()).isEqualTo("CAPITAL");           // wire "tenant"
        assertThat(row.stage()).isEqualTo(Stage.FORWARD);
        assertThat(row.ruleName()).isEqualTo("cap_data_update.MER_batch");
        assertThat(row.controlDagId()).isEqualTo("orchestration_control_dag_capital");
        assertThat(row.whenCel()).contains("context.data.status");  // wire "when"
        assertThat(row.registryVersion()).isEqualTo("v42/ab99f0");
        assertThat(row.enabled()).isTrue();                         // absent ⇒ true
    }

    @Test
    void upsert_enabledFalse_isPassedThrough_soRulesCanBeRetired() throws Exception {
        when(subscriptions.upsertAll(any(), anyString())).thenReturn(1);

        upsert(slice("""
                {"tenant":"NSFR","stage":"PERSIST","rule_name":"retired","when":"event.id != \\"\\"",
                 "registry_version":"v42/ab99f0","enabled":false}
                """)).andExpect(status().isOk());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<SubscriptionRow>> rows = ArgumentCaptor.forClass(Collection.class);
        verify(subscriptions).upsertAll(rows.capture(), anyString());
        assertThat(List.copyOf(rows.getValue()).get(0).enabled()).isFalse();
    }

    @Test
    void upsert_uncompilableCel_isBadRequest() throws Exception {
        upsert(slice("""
                {"tenant":"CAPITAL","stage":"FORWARD","rule_name":"broken",
                 "control_dag_id":"dag_capital","when":"event.foo ===","registry_version":"v1"}
                """)).andExpect(status().isBadRequest());

        verifyNoInteractions(subscriptions);
    }

    @Test
    void upsert_persistRuleReferencingContext_isBadRequest() throws Exception {
        // Amendment A4: the PERSIST gate runs pre-enrichment, so context does not exist yet. CelPrograms
        // declares it only for FORWARD, so the reference fails to compile rather than being text-matched.
        upsert(slice("""
                {"tenant":"CAPITAL","stage":"PERSIST","rule_name":"a4_violation",
                 "when":"context.data.status == \\"COMPLETE\\"","registry_version":"v1"}
                """)).andExpect(status().isBadRequest());

        verifyNoInteractions(subscriptions);
    }

    @Test
    void upsert_forwardRuleReferencingContext_isAccepted() throws Exception {
        // The A4 control: context is legitimate post-enrichment, so the same expression must pass here —
        // otherwise the rejection above would be a blanket ban rather than a stage-scoped one.
        when(subscriptions.upsertAll(any(), anyString())).thenReturn(1);

        upsert(slice(FORWARD_ROW)).andExpect(status().isOk());
    }

    @Test
    void upsert_forwardWithoutControlDagId_isBadRequest() throws Exception {
        // V3 CHECK forward_requires_dag, caught at the edge so the caller gets a 400 rather than a 500.
        upsert(slice("""
                {"tenant":"CAPITAL","stage":"FORWARD","rule_name":"no_target",
                 "when":"event.id != \\"\\"","registry_version":"v1"}
                """)).andExpect(status().isBadRequest());

        verifyNoInteractions(subscriptions);
    }

    @Test
    void upsert_unknownStage_isBadRequest() throws Exception {
        upsert(slice("""
                {"tenant":"CAPITAL","stage":"MAYBE","rule_name":"r","when":"event.id != \\"\\"",
                 "registry_version":"v1"}
                """)).andExpect(status().isBadRequest());
    }

    @Test
    void upsert_missingRequiredField_rejectsWholeSlice() throws Exception {
        String noRuleName = """
                {"tenant":"CAPITAL","stage":"PERSIST","when":"event.id != \\"\\"","registry_version":"v1"}
                """;

        upsert(slice(PERSIST_ROW, noRuleName)).andExpect(status().isBadRequest());

        verifyNoInteractions(subscriptions); // the valid sibling row is not applied either
    }

    @Test
    void upsert_missingSubscriptionsArray_isBadRequest() throws Exception {
        upsert("{}").andExpect(status().isBadRequest());
        verifyNoInteractions(subscriptions);
    }

    @Test
    void upsert_emptySlice_is200_zeroCounts() throws Exception {
        when(subscriptions.upsertAll(any(), anyString())).thenReturn(0);

        String body = upsert("{\"subscriptions\":[]}")
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        assertJsonEquals("{\"received\":0,\"upserted\":0}", body);
    }

    @Test
    void upsert_malformedJson_isBadRequest() throws Exception {
        upsert("{\"subscriptions\":").andExpect(status().isBadRequest());
    }

    // --- helpers ------------------------------------------------------------------------------------

    private ResultActions replay(String body) throws Exception {
        return mvc.perform(post("/admin/replay").contentType(MediaType.APPLICATION_JSON).content(body));
    }

    private ResultActions upsert(String body) throws Exception {
        return mvc.perform(put("/admin/subscriptions").contentType(MediaType.APPLICATION_JSON).content(body));
    }

    private static String slice(String... rows) {
        return "{\"subscriptions\":[" + String.join(",", rows) + "]}";
    }
}
