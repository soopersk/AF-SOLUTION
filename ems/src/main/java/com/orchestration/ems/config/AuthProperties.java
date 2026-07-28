package com.orchestration.ems.config;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Authentication configuration ({@code ems.auth.*}) for ems-design §4.3 "Auth". Everything here is
 * environment shape — group identifiers, claim names, fallback credentials — never a secret in the
 * repository: passwords and signing keys come from Vault/env in-cluster (§10).
 *
 * @param mode           which token world this deployment lives in — see {@link Mode}
 * @param groupsClaim    the JWT claim carrying group membership ({@code groups} for Entra; some tenants
 *                       emit {@code roles} instead)
 * @param principalClaim the claim that names the caller, and therefore what is audited into
 *                       {@code decided_by} / {@code replayed_by} / {@code updated_by}. {@code sub} by
 *                       default; a tenant issuing app-registration tokens may prefer {@code appid} or
 *                       {@code preferred_username}
 * @param groups         the three group identifiers the §4.5:201 matchers key on
 * @param users          HTTP Basic fallback accounts (empty by default — no account, no Basic access)
 * @param local          {@link Mode#LOCAL} token issuance/verification settings
 */
@ConfigurationProperties("ems.auth")
public record AuthProperties(
        @DefaultValue("local") Mode mode,
        @DefaultValue("groups") String groupsClaim,
        @DefaultValue("sub") String principalClaim,
        @DefaultValue Groups groups,
        List<BasicUser> users,
        @DefaultValue Local local) {

    public AuthProperties {
        users = users == null ? List.of() : List.copyOf(users);
    }

    /**
     * Which issuer EMS trusts. The legacy {@code AuthorizationManager} offered "Basic, Bearer/JWT via
     * Azure OIDC with group checks, or none"; this is the same choice minus "none" — an unauthenticated
     * mode is not offered, because the only reason it existed was the absence of a local issuer, which
     * {@link Mode#LOCAL} now supplies.
     */
    public enum Mode {

        /**
         * Self-issued HS256 tokens: {@code POST /token} mints them and the resource server verifies them
         * with the same key. This is what keeps dev boxes, ITs and non-Entra environments working without
         * a live IdP — it is <b>not</b> a bypass: every endpoint still requires a token and the right
         * group.
         */
        LOCAL,

        /**
         * Entra ID. EMS is a pure resource server: Boot builds the {@code JwtDecoder} from
         * {@code spring.security.oauth2.resourceserver.jwt.issuer-uri}, and startup fails fast if that is
         * missing. {@code POST /token} does not exist in this mode — Entra issues the tokens.
         */
        ENTRA
    }

    /**
     * The group identifiers whose membership grants each authority. In Entra these are group <b>object
     * ids</b> (opaque GUIDs), which is exactly why they are configuration and not constants.
     *
     * @param dispatcher may post to {@code POST /decisions} (control-DAG dispatchers and heartbeats)
     * @param admin      the elevated group for {@code /admin/*}
     * @param ci         the registry-CI principal, additionally required by {@code PUT /admin/subscriptions}
     */
    public record Groups(
            @DefaultValue("ems-dispatchers") String dispatcher,
            @DefaultValue("ems-admins") String admin,
            @DefaultValue("ems-registry-ci") String ci) {
    }

    /**
     * One HTTP Basic account.
     *
     * @param username the principal name — audited verbatim, so name these after the caller
     *                 ({@code capital_control_dag}, {@code registry-ci}), not after a person
     * @param password a {@code {noop}}/{@code {bcrypt}}-prefixed value (Spring's delegating encoder
     *                 rejects an unprefixed password rather than guessing at it)
     * @param groups   the caller's groups, mapped to authorities by the same table as a JWT's claim
     */
    public record BasicUser(String username, String password, List<String> groups) {

        public BasicUser {
            groups = groups == null ? List.of() : List.copyOf(groups);
        }
    }

    /**
     * {@link Mode#LOCAL} settings.
     *
     * @param signingKey the HS256 secret shared by {@code POST /token} and the decoder. Blank ⇒ a random
     *                   key is generated at startup, which is deliberate for a single dev process and
     *                   wrong for anything else: tokens then die with the process and are not valid on a
     *                   sibling pod
     * @param tokenTtl   how long an issued token is valid
     */
    public record Local(
            @DefaultValue("") String signingKey,
            @DefaultValue("1h") Duration tokenTtl) {
    }
}
