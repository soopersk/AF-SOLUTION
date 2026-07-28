package com.orchestration.ems.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.UserRequestPostProcessor;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import com.orchestration.ems.api.AdminController;
import com.orchestration.ems.api.GateGroupsController;
import com.orchestration.ems.api.TokenController;
import com.orchestration.ems.api.support.AbstractWebMvcTest;
import com.orchestration.ems.decisions.DecisionIngestController;
import com.orchestration.ems.decisions.RoutingDecisionRepo;
import com.orchestration.ems.ingestion.DlqReplayService;
import com.orchestration.ems.model.DlqReplay;
import com.orchestration.ems.model.GateGroups;
import com.orchestration.ems.store.GateGroupsRepository;
import com.orchestration.ems.subscription.CelPrograms;
import com.orchestration.ems.subscription.SubscriptionRepo;

/**
 * The authorization matrix of ems-design §4.5:201 — every protected surface × {anonymous, wrong group,
 * right group} → {401, 403, 2xx} — plus the local token round trip that makes {@code ems.auth.mode=local}
 * a working credential flow rather than a bypass.
 *
 * <p>One representative endpoint per authorization tier, because the tiers are what the matchers
 * distinguish: a query endpoint (any authenticated caller), {@code POST /decisions} (dispatcher),
 * {@code POST /admin/replay} (elevated group), {@code PUT /admin/subscriptions} (elevated <b>and</b> CI),
 * and {@code POST /token} (any authenticated caller).
 *
 * <p>Deliberately <b>not</b> asserted here: that {@code /actuator/health} is anonymous. The permitAll
 * matcher for it is in {@link SecurityConfig}, but actuator endpoints are not loaded in a
 * {@code @WebMvcTest} slice, so a test of it would assert the slice's routing rather than the rule.
 */
@WebMvcTest(controllers = { GateGroupsController.class, DecisionIngestController.class,
        AdminController.class, TokenController.class })
@Import({ SecurityConfig.class, CelPrograms.class })
class SecurityMatrixTest extends AbstractWebMvcTest {

    private static final String EMPTY_DECISIONS = "{\"decisions\":[]}";
    private static final String EMPTY_REPLAY = "{\"ids\":[]}";
    private static final String EMPTY_SLICE = "{\"subscriptions\":[]}";

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private GateGroupsRepository gateGroups;
    @MockitoBean
    private RoutingDecisionRepo decisions;
    @MockitoBean
    private DlqReplayService replayService;
    @MockitoBean
    private SubscriptionRepo subscriptions;

    // --- anonymous: 401 everywhere ------------------------------------------------------------------

    @Test
    void anonymous_isUnauthorizedOnEveryProtectedSurface() throws Exception {
        mvc.perform(get("/gate/groups").param("group_by", "event.id").param("lookback", "1h"))
                .andExpect(status().isUnauthorized());
        mvc.perform(json(post("/decisions"), EMPTY_DECISIONS)).andExpect(status().isUnauthorized());
        mvc.perform(json(post("/admin/replay"), EMPTY_REPLAY)).andExpect(status().isUnauthorized());
        mvc.perform(json(put("/admin/subscriptions"), EMPTY_SLICE)).andExpect(status().isUnauthorized());
        mvc.perform(post("/token")).andExpect(status().isUnauthorized());
    }

    // --- query tier: any authenticated caller -------------------------------------------------------

    @Test
    void queryEndpoint_needsAuthenticationOnly() throws Exception {
        when(gateGroups.groups(anyString(), any(), any(), any())).thenReturn(new GateGroups(List.of()));

        mvc.perform(get("/gate/groups").param("group_by", "event.id").param("lookback", "1h")
                        .with(caller("airflow-sensor")))
                .andExpect(status().isOk());
    }

    // --- POST /decisions: the dispatcher tier -------------------------------------------------------

    @Test
    void decisions_rejectsAnAdminWithoutTheDispatcherGroup() throws Exception {
        mvc.perform(json(post("/decisions"), EMPTY_DECISIONS)
                        .with(caller("ops@bank", GroupAuthorities.ADMIN)))
                .andExpect(status().isForbidden()); // authenticated, but the wrong group ⇒ 403, not 401
    }

    @Test
    void decisions_acceptsTheDispatcherGroup() throws Exception {
        mvc.perform(json(post("/decisions"), EMPTY_DECISIONS)
                        .with(caller("capital_control_dag", GroupAuthorities.DISPATCHER)))
                .andExpect(status().isOk());
    }

    // --- /admin/**: the elevated tier ---------------------------------------------------------------

    @Test
    void adminReplay_rejectsADispatcher() throws Exception {
        mvc.perform(json(post("/admin/replay"), EMPTY_REPLAY)
                        .with(caller("capital_control_dag", GroupAuthorities.DISPATCHER)))
                .andExpect(status().isForbidden());
    }

    @Test
    void adminReplay_acceptsTheElevatedGroup() throws Exception {
        when(replayService.replay(any(), anyString())).thenReturn(new DlqReplay.Result(0, 0, List.of()));

        mvc.perform(json(post("/admin/replay"), EMPTY_REPLAY)
                        .with(caller("ops@bank", GroupAuthorities.ADMIN)))
                .andExpect(status().isOk());
    }

    // --- PUT /admin/subscriptions: elevated AND CI --------------------------------------------------

    @Test
    void subscriptionUpsert_rejectsAnAdminWhoIsNotTheRegistryCi() throws Exception {
        // §4.5:201 restricts this one *additionally* to the CI principal — being elevated is not enough
        mvc.perform(json(put("/admin/subscriptions"), EMPTY_SLICE)
                        .with(caller("ops@bank", GroupAuthorities.ADMIN)))
                .andExpect(status().isForbidden());
    }

    @Test
    void subscriptionUpsert_rejectsTheCiGroupWithoutTheElevatedGroup() throws Exception {
        mvc.perform(json(put("/admin/subscriptions"), EMPTY_SLICE)
                        .with(caller("registry-ci", GroupAuthorities.CI)))
                .andExpect(status().isForbidden());
    }

    @Test
    void subscriptionUpsert_acceptsTheRegistryCiPrincipal() throws Exception {
        mvc.perform(json(put("/admin/subscriptions"), EMPTY_SLICE)
                        .with(caller("registry-ci", GroupAuthorities.ADMIN, GroupAuthorities.CI)))
                .andExpect(status().isOk());
    }

    // --- POST /token + the local credential flow ----------------------------------------------------

    @Test
    void token_hasTheStandardOAuth2ResponseShape() throws Exception {
        String body = mvc.perform(post("/token").with(caller("capital_control_dag")))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();

        var token = MAPPER.readTree(body);
        assertThat(token.path("token_type").asText()).isEqualTo("Bearer");
        assertThat(token.path("expires_in").asLong()).isEqualTo(3600L); // ems.auth.local.token-ttl=1h
        assertThat(token.path("access_token").asText().split("\\.")).hasSize(3); // header.payload.signature
    }

    @Test
    void locallyIssuedToken_isAcceptedAsABearerCredential_carryingItsGroups() throws Exception {
        // The whole local mode end to end: POST /token signs the caller's groups with the same key the
        // resource server verifies with, so the token this service mints is a credential against itself.
        String bearer = issueTokenFor("capital_control_dag", GroupAuthorities.DISPATCHER);

        mvc.perform(json(post("/decisions"), EMPTY_DECISIONS)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearer))
                .andExpect(status().isOk());
    }

    @Test
    void locallyIssuedToken_carriesNoMoreThanTheCallerHad() throws Exception {
        String bearer = issueTokenFor("capital_control_dag", GroupAuthorities.DISPATCHER);

        mvc.perform(json(post("/admin/replay"), EMPTY_REPLAY)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + bearer))
                .andExpect(status().isForbidden());
    }

    @Test
    void garbageBearerToken_isUnauthorized() throws Exception {
        mvc.perform(json(post("/decisions"), EMPTY_DECISIONS)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer not.a.token"))
                .andExpect(status().isUnauthorized());
    }

    // --- helpers ------------------------------------------------------------------------------------

    private String issueTokenFor(String username, String... authorities) throws Exception {
        String body = mvc.perform(post("/token").with(caller(username, authorities)))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        return MAPPER.readTree(body).path("access_token").asText();
    }

    /**
     * An authenticated caller holding exactly the given authorities. With none, it holds an authority
     * EMS knows nothing about — the realistic "authenticated, but in no EMS group" case.
     */
    private static UserRequestPostProcessor caller(String username, String... authorities) {
        String[] held = authorities.length == 0 ? new String[] { "NONE_OF_OURS" } : authorities;
        SimpleGrantedAuthority[] granted = new SimpleGrantedAuthority[held.length];
        for (int i = 0; i < held.length; i++) {
            granted[i] = new SimpleGrantedAuthority(held[i]);
        }
        return user(username).authorities(granted);
    }

    private static MockHttpServletRequestBuilder json(MockHttpServletRequestBuilder request, String body) {
        return request.contentType(MediaType.APPLICATION_JSON).content(body);
    }
}
