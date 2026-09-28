package com.ritikbansod.kafkawrapper.auth;

import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JwtRolesConverterTest {

    private final JwtRolesConverter converter = new JwtRolesConverter(
            new AuthProperties("oidc", List.of(), List.of(), null, null, null, null));

    @Test
    void adminClaimGrantsTheAdminRole() {
        var authentication = converter.convert(jwt(Map.of("roles", List.of("kview-admin"))));
        assertThat(authentication.getAuthorities())
                .extracting(GrantedAuthority::getAuthority)
                .containsExactly("ROLE_ADMIN");
    }

    @Test
    void anyOtherValidTokenIsReadOnly() {
        var authentication = converter.convert(jwt(Map.of("roles", List.of("viewer"))));
        assertThat(authentication.getAuthorities())
                .extracting(GrantedAuthority::getAuthority)
                .containsExactly("ROLE_READONLY");
    }

    @Test
    void missingClaimIsReadOnly() {
        var authentication = converter.convert(jwt(Map.of("sub", "someone")));
        assertThat(authentication.getAuthorities())
                .extracting(GrantedAuthority::getAuthority)
                .containsExactly("ROLE_READONLY");
    }

    @Test
    void scopeStyleStringClaimsAreParsed() {
        var converter = new JwtRolesConverter(new AuthProperties(
                "oidc", List.of(), List.of(), "scope", "kview-admin", "kview-readonly", null));
        var authentication = converter.convert(jwt(Map.of("scope", "openid kview-admin")));
        assertThat(authentication.getAuthorities())
                .extracting(GrantedAuthority::getAuthority)
                .containsExactly("ROLE_ADMIN");
    }

    private Jwt jwt(Map<String, Object> claims) {
        return new Jwt("token-value", Instant.now(), Instant.now().plusSeconds(60),
                Map.of("alg", "RS256"), claims);
    }
}
