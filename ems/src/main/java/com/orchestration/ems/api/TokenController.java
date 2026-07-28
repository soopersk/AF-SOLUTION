package com.orchestration.ems.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.orchestration.ems.config.AuthProperties;
import com.orchestration.ems.config.GroupAuthorities;

/**
 * {@code POST /token} — the legacy dev/test token endpoint (ems-design §4.3:168), carried forward for
 * <b>non-Entra environments only</b>: dev boxes, CI, and any deployment that has no IdP to issue against.
 * It exists in {@code ems.auth.mode=local} and is absent in {@code entra}, where Entra is the issuer.
 *
 * <p>The caller authenticates with HTTP Basic (the {@code ems.auth.users} accounts) and receives a
 * short-lived HS256 bearer token asserting the <em>same</em> groups that account already holds — so this
 * endpoint converts credentials into a token and never widens what the caller may do. The signing key is
 * the one the resource server verifies with, which is what makes the issued token usable against the very
 * service that minted it.
 *
 * <p>Response is the standard OAuth2 token shape (RFC 6749 §5.1) rather than a bespoke one, because every
 * HTTP client already knows how to read it.
 */
@RestController
@ConditionalOnProperty(prefix = "ems.auth", name = "mode", havingValue = "local", matchIfMissing = true)
public class TokenController {

    private final JwtEncoder encoder;
    private final AuthProperties auth;

    public TokenController(JwtEncoder encoder, AuthProperties auth) {
        this.encoder = encoder;
        this.auth = auth;
    }

    @PostMapping(value = "/token", produces = MediaType.APPLICATION_JSON_VALUE)
    public Token issue(Authentication authentication) {
        Instant now = Instant.now();
        Duration ttl = auth.local().tokenTtl();
        List<String> groups = GroupAuthorities.toGroups(authentication.getAuthorities(), auth.groups());

        JwtClaimsSet claims = JwtClaimsSet.builder()
                .issuer("ems")
                .subject(authentication.getName())
                .issuedAt(now)
                .expiresAt(now.plus(ttl))
                .claim(auth.groupsClaim(), groups)
                .build();

        String token = encoder.encode(JwtEncoderParameters.from(
                JwsHeader.with(MacAlgorithm.HS256).build(), claims)).getTokenValue();

        return new Token(token, "Bearer", ttl.toSeconds());
    }

    /**
     * RFC 6749 §5.1 token response.
     *
     * @param accessToken the signed JWT to present as {@code Authorization: Bearer …}
     * @param tokenType   always {@code Bearer}
     * @param expiresIn   lifetime in seconds ({@code ems.auth.local.token-ttl})
     */
    public record Token(
            @JsonProperty("access_token") String accessToken,
            @JsonProperty("token_type") String tokenType,
            @JsonProperty("expires_in") long expiresIn) {
    }
}
