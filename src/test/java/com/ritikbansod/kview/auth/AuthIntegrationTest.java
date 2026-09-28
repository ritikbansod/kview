package com.ritikbansod.kview.auth;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Token mode ({@code kview.auth.mode=token}): reads need any valid token, mutations
 * need the admin role, the static UI and /actuator/health stay public. Assertions on
 * mutation endpoints use an unknown cluster id, so authorization is what's exercised —
 * the call fails fast with 404 before any broker is contacted.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "kview.data-dir=target/auth-test-data",
        "kview.auth.mode=token",
        "kview.auth.tokens=admin-secret,viewer-secret:readonly"
})
class AuthIntegrationTest {

    @Autowired
    TestRestTemplate rest;

    @Test
    void apiRequiresAToken() {
        var response = rest.getForEntity("/api/clusters", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getBody()).contains("unauthorized");
    }

    @Test
    void unknownTokenIsUnauthorized() {
        assertThat(rest.exchange("/api/clusters", HttpMethod.GET, entity("not-a-token"), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void readonlyTokenCanRead() {
        assertThat(rest.exchange("/api/clusters", HttpMethod.GET, entity("viewer-secret"), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void readonlyTokenCannotMutate() {
        var response = rest.exchange("/api/clusters/does-not-exist/topics/some-topic",
                HttpMethod.DELETE, entity("viewer-secret"), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody()).contains("admin role");
    }

    @Test
    void readonlyTokenCanUseReadShapedPosts() {
        // browse is POST-shaped but a read: authorization passes, the unknown cluster 404s afterwards
        var response = rest.exchange("/api/clusters/does-not-exist/topics/some-topic/browse",
                HttpMethod.POST, entity("viewer-secret", "{\"start\":\"latest\",\"limit\":1}"), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void adminTokenCanReadAndMutate() {
        assertThat(rest.exchange("/api/clusters", HttpMethod.GET, entity("admin-secret"), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.OK);
        var mutation = rest.exchange("/api/clusters/does-not-exist/topics/some-topic",
                HttpMethod.DELETE, entity("admin-secret"), String.class);
        assertThat(mutation.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND); // past authorization, unknown cluster
    }

    @Test
    void staticUiAndHealthStayPublic() {
        assertThat(rest.getForEntity("/actuator/health", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(rest.getForEntity("/", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    private HttpEntity<String> entity(String token) {
        return entity(token, null);
    }

    private HttpEntity<String> entity(String token, String jsonBody) {
        var headers = new HttpHeaders();
        headers.setBearerAuth(token);
        if (jsonBody != null) headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(jsonBody, headers);
    }
}
