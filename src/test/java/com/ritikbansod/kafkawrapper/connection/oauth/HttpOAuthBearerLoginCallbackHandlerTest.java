package com.ritikbansod.kafkawrapper.connection.oauth;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class HttpOAuthBearerLoginCallbackHandlerTest {

    private HttpServer server;
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final AtomicReference<String> authorizationHeader = new AtomicReference<>();
    private int expiresInSeconds = 3600;

    @BeforeEach
    void startMockIdentityProvider() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/token", exchange -> {
            tokenRequests.incrementAndGet();
            authorizationHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
            String body = "{\"access_token\":\"tok-123\",\"expires_in\":" + expiresInSeconds
                    + ",\"scope\":\"kafka read\"}";
            byte[] payload = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, payload.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(payload);
            }
        });
        server.start();
    }

    @AfterEach
    void stopIdentityProvider() {
        HttpOAuthBearerLoginCallbackHandler.expirySkewMs = 30_000;
        if (server != null) {
            server.stop(0);
        }
    }

    private void configureHandler(HttpOAuthBearerLoginCallbackHandler handler) {
        Map<String, Object> options = Map.of(
                "tokenUrl", "http://127.0.0.1:" + server.getAddress().getPort() + "/token",
                "clientId", "kview",
                "clientSecret", "s3cret",
                "scope", "kafka");
        handler.configure(Map.of(), "OAUTHBEARER", List.of(new AppConfigurationEntry(
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)));
    }

    @Test
    void fetchesCachesAndRefreshesTokens() throws Exception {
        HttpOAuthBearerLoginCallbackHandler.expirySkewMs = 0; // token valid for its full lifetime

        HttpOAuthBearerLoginCallbackHandler handler = new HttpOAuthBearerLoginCallbackHandler();
        configureHandler(handler);

        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{callback});
        OAuthBearerToken token = callback.token();
        assertThat(token).isNotNull();
        assertThat(token.value()).isEqualTo("tok-123");
        assertThat(token.principalName()).isEqualTo("kview");
        assertThat(token.scope()).containsExactlyInAnyOrder("kafka", "read");
        assertThat(new String(Base64.getDecoder().decode(
                authorizationHeader.get().replaceFirst("Basic ", ""))))
                .isEqualTo("kview:s3cret");
        assertThat(tokenRequests.get()).isEqualTo(1);

        // second call within validity -> cached, no extra HTTP call
        OAuthBearerTokenCallback second = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{second});
        assertThat(second.token().value()).isEqualTo("tok-123");
        assertThat(tokenRequests.get()).isEqualTo(1);

        // proactive refresh window widened past the token's 1h lifetime -> refetched
        HttpOAuthBearerLoginCallbackHandler.expirySkewMs = 7_200_000;
        OAuthBearerTokenCallback third = new OAuthBearerTokenCallback();
        handler.handle(new Callback[]{third});
        assertThat(third.token().value()).isEqualTo("tok-123");
        assertThat(tokenRequests.get()).isEqualTo(2);
    }

    @Test
    void unsupportedCallbacksAreRejected() throws Exception {
        HttpOAuthBearerLoginCallbackHandler handler = new HttpOAuthBearerLoginCallbackHandler();
        configureHandler(handler);
        Callback unsupported = new javax.security.auth.callback.NameCallback("x");
        try {
            handler.handle(new Callback[]{unsupported});
        } catch (UnsupportedCallbackException expected) {
            assertThat(expected.getCallback()).isSameAs(unsupported);
        }
    }
}
