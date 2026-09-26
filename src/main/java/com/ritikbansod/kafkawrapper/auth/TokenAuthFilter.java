package com.ritikbansod.kafkawrapper.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static-token authentication ({@code kview.auth.mode=token}): each configured
 * entry is a raw token (admin) or {@code token:readonly}. Requests with a valid
 * bearer token get their role; everything else stays unauthenticated so the
 * entry point answers 401.
 */
final class TokenAuthFilter extends OncePerRequestFilter {

    private static final String ADMIN = "ADMIN";
    private static final String READONLY = "READONLY";

    private final Map<String, List<String>> tokensByRole = new HashMap<>();

    TokenAuthFilter(List<String> configured) {
        for (String entry : configured) {
            String trimmed = entry == null ? "" : entry.trim();
            if (trimmed.isEmpty()) continue;
            String role = ADMIN;
            int colon = trimmed.lastIndexOf(':');
            if (colon > 0) {
                String suffix = trimmed.substring(colon + 1);
                if (ADMIN.equalsIgnoreCase(suffix)) {
                    role = ADMIN;
                    trimmed = trimmed.substring(0, colon);
                } else if (READONLY.equalsIgnoreCase(suffix)) {
                    role = READONLY;
                    trimmed = trimmed.substring(0, colon);
                } // anything else is part of the token itself
            }
            tokensByRole.computeIfAbsent(trimmed, t -> new java.util.ArrayList<>()).add("ROLE_" + role);
        }
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String header = request.getHeader("Authorization");
        if (header != null && header.regionMatches(true, 0, "Bearer ", 0, 7)) {
            List<String> authorities = tokensByRole.get(header.substring(7).trim());
            if (authorities != null) {
                var authoritiesList = authorities.stream().map(SimpleAuthorities::new).toList();
                SecurityContextHolder.getContext().setAuthentication(
                        new PreAuthenticatedAuthenticationToken("kview-token", null, authoritiesList));
            }
        }
        chain.doFilter(request, response);
    }

    private record SimpleAuthorities(String role) implements org.springframework.security.core.GrantedAuthority {
        @Override
        public String getAuthority() {
            return role;
        }
    }
}
