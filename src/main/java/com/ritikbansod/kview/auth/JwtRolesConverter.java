package com.ritikbansod.kview.auth;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

import java.util.List;

/**
 * Maps a validated JWT to the Kview roles: a token carrying
 * {@code kview.auth.admin-role-value} inside {@code kview.auth.admin-role-claim}
 * is an admin; every other valid token is read-only (least privilege).
 */
final class JwtRolesConverter implements Converter<Jwt, AbstractAuthenticationToken> {

    private final AuthProperties props;

    JwtRolesConverter(AuthProperties props) {
        this.props = props;
    }

    @Override
    public AbstractAuthenticationToken convert(Jwt jwt) {
        boolean admin = claimValues(jwt).contains(props.adminRoleValue());
        var authorities = List.of(new SimpleGrantedAuthority(admin ? "ROLE_ADMIN" : "ROLE_READONLY"));
        return new JwtAuthenticationToken(jwt, authorities);
    }

    /** Reads the configured claim as a list, tolerating a single string or a space/comma list (scope style). */
    private List<String> claimValues(Jwt jwt) {
        Object value = jwt.getClaims().get(props.adminRoleClaim());
        if (value instanceof List<?> list) return list.stream().map(String::valueOf).toList();
        if (value instanceof String s && !s.isBlank()) return List.of(s.split("[\\s,]+"));
        return List.of();
    }
}
