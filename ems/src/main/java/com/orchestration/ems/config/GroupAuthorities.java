package com.orchestration.ems.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.orchestration.ems.config.AuthProperties.Groups;

/**
 * The one translation between <b>group membership</b> (what an identity provider asserts) and
 * <b>authority</b> (what the §4.5:201 request matchers check).
 *
 * <p>It is a single table used in three places — the JWT claim converter, the HTTP Basic account
 * factory, and {@code POST /token} issuance — so a Basic caller and a JWT caller in the same group get
 * exactly the same permissions, and a token this service issues is understood by the rules that guard
 * it. Groups that map to nothing are dropped silently: a caller normally belongs to many groups that
 * have nothing to do with EMS.
 */
public final class GroupAuthorities {

    /** May post to {@code POST /decisions} — the dispatcher/heartbeat identity (§4.5:219). */
    public static final String DISPATCHER = "EMS_DISPATCHER";

    /** The elevated group required by every {@code /admin/*} endpoint (§4.5:220). */
    public static final String ADMIN = "EMS_ADMIN";

    /** The registry-CI principal additionally required by {@code PUT /admin/subscriptions} (§4.5:221). */
    public static final String CI = "EMS_CI";

    private GroupAuthorities() {
    }

    /**
     * Map asserted group identifiers to EMS authorities.
     *
     * @param assertedGroups the caller's groups (a JWT claim, or a Basic account's configured list);
     *                       {@code null} is treated as "no groups"
     * @param mapping        the configured group identifiers
     * @return the granted authorities, without duplicates
     */
    public static List<GrantedAuthority> from(Collection<String> assertedGroups, Groups mapping) {
        if (assertedGroups == null || assertedGroups.isEmpty()) {
            return List.of();
        }
        Set<String> names = new LinkedHashSet<>();
        for (String group : assertedGroups) {
            if (group == null) {
                continue;
            }
            String trimmed = group.trim();
            if (trimmed.equals(mapping.dispatcher())) {
                names.add(DISPATCHER);
            }
            if (trimmed.equals(mapping.admin())) {
                names.add(ADMIN);
            }
            if (trimmed.equals(mapping.ci())) {
                names.add(CI);
            }
        }
        List<GrantedAuthority> authorities = new ArrayList<>(names.size());
        names.forEach(name -> authorities.add(new SimpleGrantedAuthority(name)));
        return List.copyOf(authorities);
    }

    /**
     * The inverse: the group identifiers to write into a locally-issued token's claim, so that decoding
     * it restores the authorities the caller had when it asked for the token.
     *
     * @param authorities the authenticated caller's authorities
     * @param mapping     the configured group identifiers
     * @return the group identifiers to assert in the token
     */
    public static List<String> toGroups(Collection<? extends GrantedAuthority> authorities, Groups mapping) {
        List<String> groups = new ArrayList<>(3);
        for (GrantedAuthority authority : authorities) {
            switch (authority.getAuthority()) {
                case DISPATCHER -> groups.add(mapping.dispatcher());
                case ADMIN -> groups.add(mapping.admin());
                case CI -> groups.add(mapping.ci());
                default -> { /* not an EMS authority — nothing to assert */ }
            }
        }
        return List.copyOf(groups);
    }
}
