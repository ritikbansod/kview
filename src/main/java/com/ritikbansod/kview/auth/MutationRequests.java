package com.ritikbansod.kview.auth;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;

import java.util.List;

/**
 * Matches mutation requests under {@code /api}: every non-GET except the
 * read-shaped POSTs (browse, decode, encode, test). One definition shared by
 * the security chain (admin-only rule), the read-only-mode filter and the
 * audit trail.
 */
final class MutationRequests implements RequestMatcher {

    static final MutationRequests INSTANCE = new MutationRequests();

    private static final PathMatcher ANT = new AntPathMatcher();
    private static final List<String> READ_SHAPED_POSTS =
            List.of("/**/browse", "/**/decode", "/**/encode", "/**/test", "/**/search");

    private MutationRequests() { }

    @Override
    public boolean matches(HttpServletRequest request) {
        String method = request.getMethod();
        if ("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)
                || "OPTIONS".equalsIgnoreCase(method)) {
            return false;
        }
        String path = request.getRequestURI();
        if (!path.startsWith("/api/")) return false;
        if (!"POST".equalsIgnoreCase(method)) return true;
        return READ_SHAPED_POSTS.stream().noneMatch(pattern -> ANT.match(pattern, path));
    }
}
