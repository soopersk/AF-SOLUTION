package com.orchestration.ems.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import com.orchestration.ems.config.AuthProperties.Groups;

/**
 * The group → authority table (ems-design §4.5:201). This is the single point where an identity
 * provider's assertion becomes an EMS permission, so it is unit-tested directly rather than only through
 * the filter chain: a bug here either locks every caller out or hands {@code /admin/*} to anyone.
 */
class GroupAuthoritiesTest {

    private static final Groups MAPPING = new Groups("ems-dispatchers", "ems-admins", "ems-registry-ci");

    @Test
    void mapsEachConfiguredGroupToItsAuthority() {
        assertThat(names(GroupAuthorities.from(List.of("ems-dispatchers"), MAPPING)))
                .containsExactly(GroupAuthorities.DISPATCHER);
        assertThat(names(GroupAuthorities.from(List.of("ems-admins"), MAPPING)))
                .containsExactly(GroupAuthorities.ADMIN);
        assertThat(names(GroupAuthorities.from(List.of("ems-registry-ci"), MAPPING)))
                .containsExactly(GroupAuthorities.CI);
    }

    @Test
    void grantsEveryMatchingAuthority_forAMultiGroupPrincipal() {
        // the registry CI holds both: PUT /admin/subscriptions needs EMS_ADMIN *and* EMS_CI
        List<GrantedAuthority> granted =
                GroupAuthorities.from(List.of("ems-admins", "ems-registry-ci"), MAPPING);

        assertThat(names(granted)).containsExactly(GroupAuthorities.ADMIN, GroupAuthorities.CI);
    }

    @Test
    void ignoresGroupsThatMeanNothingToEms() {
        // a real Entra principal is in dozens of groups; only the three configured ones matter
        assertThat(GroupAuthorities.from(
                List.of("finance-all", "vpn-users", "ems-dispatchers", "printer-admins"), MAPPING))
                .extracting(GrantedAuthority::getAuthority)
                .containsExactly(GroupAuthorities.DISPATCHER);
    }

    @Test
    void absentOrEmptyClaim_grantsNothing() {
        assertThat(GroupAuthorities.from(null, MAPPING)).isEmpty();   // the claim was missing entirely
        assertThat(GroupAuthorities.from(List.of(), MAPPING)).isEmpty();
        assertThat(GroupAuthorities.from(Arrays.asList((String) null), MAPPING)).isEmpty();
    }

    @Test
    void trimsWhitespace_andDeduplicates() {
        assertThat(names(GroupAuthorities.from(List.of(" ems-admins ", "ems-admins"), MAPPING)))
                .containsExactly(GroupAuthorities.ADMIN);
    }

    @Test
    void toGroups_isTheInverse_soAnIssuedTokenRestoresTheSameAuthorities() {
        List<GrantedAuthority> original =
                GroupAuthorities.from(List.of("ems-dispatchers", "ems-admins"), MAPPING);

        List<String> asserted = GroupAuthorities.toGroups(original, MAPPING);

        assertThat(asserted).containsExactlyInAnyOrder("ems-dispatchers", "ems-admins");
        assertThat(GroupAuthorities.from(asserted, MAPPING)).isEqualTo(original);
    }

    @Test
    void toGroups_assertsNothingForNonEmsAuthorities() {
        assertThat(GroupAuthorities.toGroups(
                List.of(new SimpleGrantedAuthority("ROLE_USER")), MAPPING)).isEmpty();
    }

    private static List<String> names(List<GrantedAuthority> authorities) {
        return authorities.stream().map(GrantedAuthority::getAuthority).toList();
    }
}
