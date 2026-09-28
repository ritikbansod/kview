package com.ritikbansod.kview.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Append-only JSONL audit trail ({@code kview.audit=true}): one line per
 * mutation request — who sent it (X-Kview-Client / User-Agent), what (method +
 * full path, which includes the cluster and topic) and how it ended (status).
 * Message payloads are never logged.
 */
final class AuditFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(AuditFilter.class);
    private static final String CLIENT_HEADER = "X-Kview-Client";

    private final Path file;
    private final ObjectMapper mapper;

    AuditFilter(String dataDir, ObjectMapper mapper) {
        this.file = Path.of(dataDir, "audit.log");
        this.mapper = mapper;
        log.info("Kview audit trail is ON — mutations are appended to {}", file);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        try {
            chain.doFilter(request, response);
        } finally {
            record(request, response);
        }
    }

    private void record(HttpServletRequest request, HttpServletResponse response) {
        if (!MutationRequests.INSTANCE.matches(request)) return;
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("ts", Instant.now().toString());
        entry.put("method", request.getMethod());
        entry.put("path", request.getRequestURI());
        if (request.getQueryString() != null) entry.put("query", request.getQueryString());
        entry.put("status", response.getStatus());
        entry.put("client", clientOf(request));
        try {
            synchronized (this) {
                Files.createDirectories(file.getParent());
                Files.writeString(file, mapper.writeValueAsString(entry) + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            log.warn("Failed to append to the audit trail {}: {}", file, e.getMessage());
        }
    }

    private static String clientOf(HttpServletRequest request) {
        String client = request.getHeader(CLIENT_HEADER);
        if (client != null && !client.isBlank()) return client;
        String userAgent = request.getHeader("User-Agent");
        return userAgent == null || userAgent.isBlank() ? "unknown" : userAgent;
    }
}
