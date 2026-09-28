package com.ritikbansod.kview.auth;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Zero-config contract: with no kview.auth.* settings (the default), the API
 * behaves exactly as before — open, so a localhost single-user setup keeps
 * working and every existing integration (CLI, MCP, scripts) is unaffected.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "kview.data-dir=target/auth-none-test-data")
class NoAuthByDefaultTest {

    @Autowired
    TestRestTemplate rest;

    @Test
    void apiStaysOpenWithoutConfiguration() {
        assertThat(rest.getForEntity("/api/clusters", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void mutationsAreNotBlockedByAuth() {
        // unknown cluster id fails fast with 404 — the point is that auth did not intercept
        var response = rest.exchange("/api/clusters/does-not-exist/topics/some-topic",
                HttpMethod.DELETE, HttpEntity.EMPTY, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
