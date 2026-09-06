package com.ritikbansod.kafkawrapper.connection.oauth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SASL/OAUTHBEARER login handler implementing the OAuth2 client-credentials grant
 * against any token endpoint (Keycloak, Entra ID, Okta, Auth0, ...).
 *
 * <p>Configured purely through the JAAS options carried in the profile, e.g.:
 * <pre>
 * org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
 *     tokenUrl="https://idp/token" clientId="wrapper" clientSecret="..." scope="kafka";
 * </pre>
 * Tokens are cached and refreshed before expiry.
 */
public class HttpOAuthBearerLoginCallbackHandler implements AuthenticateCallbackHandler {

    static final String TOKEN_URL_OPTION = "tokenUrl";
    static final String CLIENT_ID_OPTION = "clientId";
    static final String CLIENT_SECRET_OPTION = "clientSecret";
    static final String SCOPE_OPTION = "scope";

    private static final Logger log = LoggerFactory.getLogger(HttpOAuthBearerLoginCallbackHandler.class);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(10);

    /** Tokens are proactively refreshed this many ms before their real expiry. */
    static long expirySkewMs = 30_000;

    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http;
    private String tokenUrl;
    private String clientId;
    private String clientSecret;
    private String scope;
    private volatile CachedToken cached;

    public HttpOAuthBearerLoginCallbackHandler() {
        this(false);
    }

    /** Used by tests (and profiles with hostname verification disabled) to allow plain-HTTP or self-signed endpoints. */
    public HttpOAuthBearerLoginCallbackHandler(boolean trustAll) {
        HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT);
        if (trustAll) {
            builder.sslContext(trustAllContext());
        }
        this.http = builder.build();
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism,
                          List<AppConfigurationEntry> jaasConfigEntries) {
        if (jaasConfigEntries == null || jaasConfigEntries.isEmpty()) {
            throw new IllegalArgumentException("OAUTHBEARER login requires JAAS configuration entries");
        }
        Map<String, ?> options = jaasConfigEntries.get(0).getOptions();
        this.tokenUrl = required(options, TOKEN_URL_OPTION);
        this.clientId = required(options, CLIENT_ID_OPTION);
        this.clientSecret = required(options, CLIENT_SECRET_OPTION);
        this.scope = optional(options, SCOPE_OPTION);
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback tokenCallback) {
                try {
                    tokenCallback.token(validToken());
                } catch (RuntimeException e) {
                    tokenCallback.error("token_fetch_failed", e.getMessage(), null);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() {
        // nothing to release; the HttpClient holds no per-login state
    }

    OAuthBearerToken validToken() {
        CachedToken current = cached;
        if (current != null && current.expiresAtMs() - expirySkewMs > System.currentTimeMillis()) {
            return current.token();
        }
        synchronized (this) {
            current = cached;
            if (current != null && current.expiresAtMs() - expirySkewMs > System.currentTimeMillis()) {
                return current.token();
            }
            CachedToken fresh = fetchToken();
            cached = fresh;
            return fresh.token();
        }
    }

    private CachedToken fetchToken() {
        try {
            String form = scope == null || scope.isBlank()
                    ? "grant_type=client_credentials"
                    : "grant_type=client_credentials&scope=" + java.net.URLEncoder.encode(scope, StandardCharsets.UTF_8);
            String basic = Base64.getEncoder().encodeToString(
                    (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

            HttpRequest request = HttpRequest.newBuilder(URI.create(tokenUrl))
                    .timeout(HTTP_TIMEOUT)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .header("Authorization", "Basic " + basic)
                    .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new IllegalStateException("Token endpoint " + tokenUrl + " returned HTTP "
                        + response.statusCode() + ": " + truncate(response.body()));
            }
            JsonNode body = mapper.readTree(response.body());
            String accessToken = body.path("access_token").asText(null);
            if (accessToken == null || accessToken.isBlank()) {
                throw new IllegalStateException("Token endpoint response did not contain an access_token");
            }
            long lifetimeMs = System.currentTimeMillis() + body.path("expires_in").asLong(3600) * 1000;
            OAuthBearerToken token = new ClientCredentialsToken(accessToken, clientId, lifetimeMs, parseScopes(body));
            log.info("Obtained OAuth token for client '{}' (valid until epoch {})", clientId, lifetimeMs);
            return new CachedToken(token, lifetimeMs);
        } catch (IOException e) {
            throw new IllegalStateException("Could not reach token endpoint " + tokenUrl + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while fetching OAuth token", e);
        }
    }

    private static Set<String> parseScopes(JsonNode body) {
        Set<String> scopes = new LinkedHashSet<>();
        JsonNode scopeNode = body.path("scope");
        if (scopeNode.isTextual()) {
            for (String s : scopeNode.asText().split("\\s+")) {
                if (!s.isBlank()) {
                    scopes.add(s);
                }
            }
        } else if (scopeNode.isArray()) {
            scopeNode.forEach(n -> scopes.add(n.asText()));
        }
        return scopes;
    }

    private static String required(Map<String, ?> options, String key) {
        String value = optional(options, key);
        if (value == null) {
            throw new IllegalArgumentException("OAUTHBEARER JAAS options are missing '" + key + "'");
        }
        return value;
    }

    private static String optional(Map<String, ?> options, String key) {
        Object value = options.get(key);
        return value == null ? null : String.valueOf(value);
    }

    private static String truncate(String s) {
        return s == null ? "" : s.substring(0, Math.min(s.length(), 300));
    }

    private static SSLContext trustAllContext() {
        TrustManager permissive = new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) { }
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) { }
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        };
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, new TrustManager[]{permissive}, new SecureRandom());
            return context;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private record CachedToken(OAuthBearerToken token, long expiresAtMs) { }

    record ClientCredentialsToken(String value, String principal, long lifetimeMs, Set<String> scopes)
            implements OAuthBearerToken {
        @Override public String value() { return value; }
        @Override public String principalName() { return principal; }
        @Override public Long startTimeMs() { return 0L; }
        @Override public long lifetimeMs() { return lifetimeMs; }
        @Override public Set<String> scope() { return scopes; }
    }
}
