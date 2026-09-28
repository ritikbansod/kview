package com.ritikbansod.kview.auth;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * API authentication settings ({@code kview.auth.*}).
 *
 * <ul>
 *   <li>{@code mode=none} (default) — no authentication; safe for the default
 *       loopback binding on a single-user machine.</li>
 *   <li>{@code mode=token} — static bearer tokens from {@code tokens}
 *       ("token" = admin, "token:readonly" = read-only).</li>
 *   <li>{@code mode=oidc} — JWT bearer tokens validated against the OAuth2/OIDC
 *       issuer configured via {@code spring.security.oauth2.resourceserver.jwt.*};
 *       the {@code adminRoleClaim} decides admin vs read-only.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "kview.auth")
public record AuthProperties(
        String mode,
        List<String> tokens,
        List<String> allowedOrigins,
        String adminRoleClaim,
        String adminRoleValue,
        String readonlyRoleValue,
        String audience) {

    public static final String MODE_NONE = "none";
    public static final String MODE_TOKEN = "token";
    public static final String MODE_OIDC = "oidc";

    public AuthProperties {
        mode = mode == null || mode.isBlank() ? MODE_NONE : mode.toLowerCase();
        tokens = tokens == null ? List.of() : tokens;
        allowedOrigins = allowedOrigins == null ? List.of() : allowedOrigins;
        adminRoleClaim = adminRoleClaim == null || adminRoleClaim.isBlank() ? "roles" : adminRoleClaim;
        adminRoleValue = adminRoleValue == null || adminRoleValue.isBlank() ? "kview-admin" : adminRoleValue;
        readonlyRoleValue = readonlyRoleValue == null || readonlyRoleValue.isBlank() ? "kview-readonly" : readonlyRoleValue;
    }
}
