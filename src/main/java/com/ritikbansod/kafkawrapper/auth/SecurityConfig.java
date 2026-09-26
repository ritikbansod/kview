package com.ritikbansod.kafkawrapper.auth;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.PathMatcher;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import java.io.IOException;
import java.util.List;

/**
 * API authentication ({@code kview.auth.mode}, default {@code none}):
 * <ul>
 *   <li>none — everything open (safe on the default loopback binding, zero config);</li>
 *   <li>token — static bearer tokens with admin / read-only roles;</li>
 *   <li>oidc — JWTs from any OIDC issuer, role from a configurable claim.</li>
 * </ul>
 * When authentication is on, reads under {@code /api/**} need any role, mutations
 * need admin, CORS is closed unless {@code kview.auth.allowed-origins} says otherwise,
 * and {@code /}, the static UI and {@code /actuator/health} stay public.
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(AuthProperties.class)
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    /** POST endpoints that read, not mutate — a read-only role may call these. */
    private static final List<String> READ_SHAPED_POSTS = List.of("/**/browse", "/**/decode", "/**/encode", "/**/test");

    private final AuthProperties auth;

    public SecurityConfig(AuthProperties auth) {
        this.auth = auth;
        if (auth.mode().equals(AuthProperties.MODE_TOKEN) && auth.tokens().isEmpty()) {
            log.warn("kview.auth.mode=token but no kview.auth.tokens configured — every request will be rejected");
        }
        if (!auth.mode().equals(AuthProperties.MODE_NONE)) {
            log.info("Kview API authentication is ON (mode {}, mutations need the admin role)", auth.mode());
        }
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(AbstractHttpConfigurer::disable);
        http.cors(Customizer.withDefaults());
        http.sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        if (auth.mode().equals(AuthProperties.MODE_NONE)) {
            http.authorizeHttpRequests(a -> a.anyRequest().permitAll());
            return http.build();
        }

        http.authorizeHttpRequests(a -> a
                        .requestMatchers("/", "/index.html", "/css/**", "/js/**", "/favicon.ico", "/error",
                                "/actuator/health").permitAll()
                        .requestMatchers("/actuator/**").hasAnyRole("ADMIN", "READONLY")
                        .requestMatchers(MutationMatcher.INSTANCE).hasRole("ADMIN")
                        .requestMatchers("/api/**").hasAnyRole("ADMIN", "READONLY")
                        .anyRequest().permitAll())
                .exceptionHandling(e -> e
                        .authenticationEntryPoint(jsonEntryPoint())
                        .accessDeniedHandler(jsonDeniedHandler()));

        http.addFilterBefore(new AccessTokenParamFilter(), UsernamePasswordAuthenticationFilter.class);
        switch (auth.mode()) {
            case AuthProperties.MODE_TOKEN ->
                    http.addFilterBefore(new TokenAuthFilter(auth.tokens()), UsernamePasswordAuthenticationFilter.class);
            case AuthProperties.MODE_OIDC -> http.oauth2ResourceServer(rs ->
                    rs.jwt(jwt -> jwt.jwtAuthenticationConverter(new JwtRolesConverter(auth))));
            default -> throw new IllegalStateException(
                    "kview.auth.mode must be none | token | oidc — got '" + auth.mode() + "'");
        }
        return http.build();
    }

    /** Issuer/JWK configuration comes from spring.security.oauth2.resourceserver.jwt.*; audience check is optional. */
    @Bean
    @org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(name = "kview.auth.mode", havingValue = "oidc")
    public JwtDecoder jwtDecoder(
            @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri:}") String issuerUri,
            @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri:}") String jwkSetUri) {
        NimbusJwtDecoder decoder;
        if (!jwkSetUri.isBlank()) {
            decoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri).build();
        } else if (!issuerUri.isBlank()) {
            decoder = (NimbusJwtDecoder) JwtDecoders.fromIssuerLocation(issuerUri);
        } else {
            throw new IllegalStateException(
                    "kview.auth.mode=oidc needs spring.security.oauth2.resourceserver.jwt.issuer-uri (or jwk-set-uri)");
        }
        if (auth.audience() != null && !auth.audience().isBlank()) {
            String expected = auth.audience();
            OAuth2TokenValidator<Jwt> audience = jwt -> {
                Object aud = jwt.getClaims().get("aud");
                boolean ok = expected.equals(aud) || (aud instanceof List<?> list && list.contains(expected));
                return ok ? OAuth2TokenValidatorResult.success()
                        : OAuth2TokenValidatorResult.failure(new OAuth2Error(
                                "invalid_token", "Token audience does not include '" + expected + "'", null));
            };
            decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(JwtValidators.createDefault(), audience));
        }
        return decoder;
    }

    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        if (auth.mode().equals(AuthProperties.MODE_NONE)) {
            // local development (e.g. a Vite dev server talking to the API directly)
            config.setAllowedOriginPatterns(List.of("*"));
            config.setAllowedHeaders(List.of("*"));
        } else {
            // authenticated: no wildcard — opt in per origin
            config.setAllowedOrigins(auth.allowedOrigins());
            config.setAllowedHeaders(List.of("Authorization", "Content-Type"));
        }
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/api/**", config);
        return source;
    }

    /** Mutations under /api: every non-GET except the read-shaped POSTs (browse, decode, encode, test). */
    static final class MutationMatcher implements RequestMatcher {

        static final MutationMatcher INSTANCE = new MutationMatcher();
        private static final PathMatcher ANT = new AntPathMatcher();

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

    private static org.springframework.security.web.AuthenticationEntryPoint jsonEntryPoint() {
        return (request, response, authException) -> writeJson(response, HttpServletResponse.SC_UNAUTHORIZED,
                "unauthorized", "Authentication required — send 'Authorization: Bearer <token>' "
                        + "(see the Authentication section in the README).");
    }

    private static org.springframework.security.web.access.AccessDeniedHandler jsonDeniedHandler() {
        return (request, response, accessDeniedException) -> writeJson(response, HttpServletResponse.SC_FORBIDDEN,
                "forbidden", "This operation requires the admin role.");
    }

    private static void writeJson(HttpServletResponse response, int status, String error, String detail)
            throws IOException {
        response.setStatus(status);
        response.setHeader("WWW-Authenticate", "Bearer");
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write("{\"error\":\"" + error + "\",\"detail\":\"" + detail + "\"}");
    }
}
