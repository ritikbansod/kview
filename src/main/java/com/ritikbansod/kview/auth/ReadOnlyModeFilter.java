package com.ritikbansod.kview.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Global read-only mode ({@code kview.readonly=true}): every mutation is
 * rejected with 403 — for all clients (UI, CLI, MCP) and all roles. Registered
 * ahead of the security chain so the reason is "read-only mode" even without
 * credentials.
 */
final class ReadOnlyModeFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (MutationRequests.INSTANCE.matches(request)) {
            JsonError.write(response, HttpServletResponse.SC_FORBIDDEN, "forbidden",
                    "Kview is running in read-only mode (KVIEW_READONLY=true) — mutations are disabled.");
            return;
        }
        chain.doFilter(request, response);
    }
}
