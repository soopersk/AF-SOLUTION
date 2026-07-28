package com.orchestration.ems.config;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.authorization.AuthorizationManager;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.RequestAuthorizationContext;

import com.nimbusds.jose.jwk.source.ImmutableSecret;

import com.orchestration.ems.config.AuthProperties.BasicUser;
import com.orchestration.ems.config.AuthProperties.Mode;

/**
 * The EMS security chain (ems-design §4.3 "Auth", §4.5:201): an OAuth2 resource server verifying Entra
 * JWTs with group-claim → authority mapping, plus an HTTP Basic fallback — the same three modes the
 * legacy {@code AuthorizationManager} offered, minus "none".
 *
 * <p><b>The authorization matrix</b> is §4.5:201 read literally:
 * <table border="1">
 *   <caption>request matchers</caption>
 *   <tr><th>Endpoint</th><th>Requires</th></tr>
 *   <tr><td>{@code /actuator/health/**}, {@code /actuator/info}</td><td>nothing (K8s probes, §10)</td></tr>
 *   <tr><td>{@code PUT /admin/subscriptions}</td><td>{@code EMS_ADMIN} <b>and</b> {@code EMS_CI}</td></tr>
 *   <tr><td>{@code /admin/**}</td><td>{@code EMS_ADMIN} ("an elevated group")</td></tr>
 *   <tr><td>{@code POST /decisions}</td><td>{@code EMS_DISPATCHER}</td></tr>
 *   <tr><td>everything else ({@code /event}, {@code /context}, {@code /run/status}, …)</td>
 *       <td>any authenticated caller</td></tr>
 * </table>
 * The subscriptions rule requires <em>both</em> authorities because §4.5:201 says the CI principal is an
 * <em>additional</em> restriction on top of the elevated group, not a substitute for it.
 *
 * <p><b>Stateless and CSRF-free by design.</b> There is no session and no cookie — every caller presents a
 * bearer token or Basic credentials on every request — so CSRF protection would guard against an attack
 * that cannot happen here while breaking every non-browser client.
 *
 * <p><b>{@link Mode#LOCAL} is the default, and that is a deliberate trade with a loud warning.</b> Making
 * {@link Mode#ENTRA} the default would fail every {@code @WebMvcTest} slice and dev box that has no IdP;
 * making LOCAL silent would let a shared environment accept self-issued tokens unnoticed. So the default
 * keeps development working and {@link #localSigningKey} logs a warning naming the exact property to set.
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(AuthProperties.class)
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Bean
    public SecurityFilterChain apiSecurity(HttpSecurity http, AuthProperties auth) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers("/actuator/health/**", "/actuator/info").permitAll()
                        .requestMatchers(HttpMethod.PUT, "/admin/subscriptions").access(registryCi())
                        .requestMatchers("/admin/**").hasAuthority(GroupAuthorities.ADMIN)
                        .requestMatchers(HttpMethod.POST, "/decisions")
                                .hasAuthority(GroupAuthorities.DISPATCHER)
                        .anyRequest().authenticated())
                .httpBasic(Customizer.withDefaults())
                .oauth2ResourceServer(oauth2 ->
                        oauth2.jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter(auth))));
        return http.build();
    }

    /** {@code EMS_ADMIN} <b>and</b> {@code EMS_CI} — see the class javadoc on "additionally restricted". */
    private static AuthorizationManager<RequestAuthorizationContext> registryCi() {
        return (authentication, context) -> {
            Authentication caller = authentication.get();
            boolean granted = caller != null && caller.isAuthenticated()
                    && has(caller, GroupAuthorities.ADMIN) && has(caller, GroupAuthorities.CI);
            return new AuthorizationDecision(granted);
        };
    }

    private static boolean has(Authentication caller, String authority) {
        for (GrantedAuthority granted : caller.getAuthorities()) {
            if (authority.equals(granted.getAuthority())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Turn a verified JWT into an authenticated caller: authorities from the configured groups claim, and
     * a principal name from the configured principal claim.
     *
     * <p>The name matters beyond logging — it is what gets audited into {@code decided_by},
     * {@code replayed_by} and {@code updated_by} — so it falls back to {@code sub} when the configured
     * claim is absent from a token. Without that fallback a misconfigured claim name would yield a
     * nameless principal and a {@code NOT NULL} violation at write time, which is a confusing way to
     * learn about a typo in configuration.
     */
    private static Converter<Jwt, AbstractAuthenticationToken> jwtAuthenticationConverter(
            AuthProperties auth) {
        return jwt -> {
            Collection<GrantedAuthority> authorities =
                    GroupAuthorities.from(jwt.getClaimAsStringList(auth.groupsClaim()), auth.groups());
            String claimed = jwt.getClaimAsString(auth.principalClaim());
            String name = (claimed == null || claimed.isBlank()) ? jwt.getSubject() : claimed;
            return new JwtAuthenticationToken(jwt, authorities, name);
        };
    }

    /**
     * The HTTP Basic accounts, whose groups run through the same {@link GroupAuthorities} table as a JWT's
     * claim — a Basic caller and a token caller in the same group are equally privileged, never more.
     *
     * <p>Declaring this bean (even with no accounts configured, which is the default) also suppresses
     * Boot's generated-password user: an EMS deployment grants access only to identities someone wrote
     * down.
     */
    @Bean
    public UserDetailsService emsBasicUsers(AuthProperties auth) {
        List<UserDetails> accounts = new ArrayList<>();
        for (BasicUser user : auth.users()) {
            accounts.add(User.withUsername(user.username())
                    .password(user.password())
                    .authorities(GroupAuthorities.from(user.groups(), auth.groups()))
                    .build());
        }
        if (accounts.isEmpty()) {
            log.info("no ems.auth.users configured — HTTP Basic is unusable, bearer tokens only");
        }
        return new InMemoryUserDetailsManager(accounts);
    }

    /** Delegating encoder: configured passwords carry a {@code {noop}}/{@code {bcrypt}} prefix. */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }

    /**
     * {@link Mode#LOCAL} token plumbing: one HS256 secret shared by the {@code POST /token} issuer and the
     * decoder that verifies what it issued. Absent in {@link Mode#ENTRA}, where Boot builds the decoder
     * from {@code spring.security.oauth2.resourceserver.jwt.issuer-uri} — and startup fails fast if that
     * property is missing, which is the correct outcome for a deployment that claims Entra but has no
     * issuer.
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "ems.auth", name = "mode", havingValue = "local", matchIfMissing = true)
    public static class LocalTokens {

        /** HS256 needs a 256-bit key; a shorter configured secret is a configuration error, not a warning. */
        private static final int MIN_KEY_BYTES = 32;

        @Bean
        public SecretKey localSigningKey(AuthProperties auth) {
            String configured = auth.local().signingKey();
            if (configured == null || configured.isBlank()) {
                log.warn("ems.auth.mode=local with no ems.auth.local.signing-key: EMS is issuing and "
                        + "accepting its own tokens with a key generated for this process only. Set "
                        + "ems.auth.mode=entra (plus spring.security.oauth2.resourceserver.jwt.issuer-uri) "
                        + "in any shared environment.");
                return generateKey();
            }
            byte[] secret = configured.getBytes(StandardCharsets.UTF_8);
            if (secret.length < MIN_KEY_BYTES) {
                throw new IllegalStateException("ems.auth.local.signing-key must be at least "
                        + MIN_KEY_BYTES + " characters for HS256 (was " + secret.length + ")");
            }
            return new SecretKeySpec(secret, "HmacSHA256");
        }

        private static SecretKey generateKey() {
            try {
                return KeyGenerator.getInstance("HmacSHA256").generateKey();
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("HmacSHA256 is required by every JRE", impossible);
            }
        }

        @Bean
        public JwtDecoder localJwtDecoder(SecretKey localSigningKey) {
            return NimbusJwtDecoder.withSecretKey(localSigningKey)
                    .macAlgorithm(MacAlgorithm.HS256)
                    .build();
        }

        @Bean
        public JwtEncoder localJwtEncoder(SecretKey localSigningKey) {
            return new NimbusJwtEncoder(new ImmutableSecret<>(localSigningKey));
        }
    }
}
