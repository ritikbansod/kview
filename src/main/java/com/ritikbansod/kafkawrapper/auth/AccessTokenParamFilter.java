package com.ritikbansod.kafkawrapper.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * Lets SSE clients (the browser's EventSource cannot send headers) present a
 * bearer token via {@code ?access_token=...} — the parameter is turned into the
 * Authorization header the rest of the chain expects. Real-header requests are
 * left untouched.
 */
final class AccessTokenParamFilter extends OncePerRequestFilter {

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String token = request.getParameter("access_token");
        if (token == null || token.isBlank() || request.getHeader("Authorization") != null) {
            chain.doFilter(request, response);
            return;
        }
        chain.doFilter(new BearerHeaderRequest(request, token), response);
    }

    private static final class BearerHeaderRequest extends HttpServletRequestWrapper {

        private BearerHeaderRequest(HttpServletRequest delegate, String token) {
            super(wrap(delegate, token));
        }

        private static HttpServletRequest wrap(HttpServletRequest delegate, String token) {
            String value = "Bearer " + token;
            return new HttpServletRequestWrapper(delegate) {
                @Override
                public String getHeader(String name) {
                    if ("Authorization".equalsIgnoreCase(name)) return value;
                    return super.getHeader(name);
                }

                @Override
                public Enumeration<String> getHeaders(String name) {
                    if ("Authorization".equalsIgnoreCase(name)) return Collections.enumeration(List.of(value));
                    return super.getHeaders(name);
                }

                @Override
                public Enumeration<String> getHeaderNames() {
                    List<String> names = new java.util.ArrayList<>(Collections.list(super.getHeaderNames()));
                    if (!names.contains("Authorization")) names.add("Authorization");
                    return Collections.enumeration(names);
                }
            };
        }
    }
}
