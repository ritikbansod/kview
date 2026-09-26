package com.ritikbansod.kafkawrapper.auth;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Global read-only mode ({@code KVIEW_READONLY=true}): every mutation is
 * rejected — in every auth mode, for every role — while reads (including the
 * read-shaped POSTs) keep working.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "kview.data-dir=target/readonly-test-data",
        "kview.readonly=true"
})
class ReadOnlyModeTest {

    @Autowired
    TestRestTemplate rest;

    @Test
    void mutationsAreRejectedWithTheReadOnlyReason() {
        var response = rest.exchange("/api/clusters/does-not-exist/topics/some-topic",
                HttpMethod.DELETE, HttpEntity.EMPTY, String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(response.getBody()).contains("read-only mode");
    }

    @Test
    void readsStillWork() {
        assertThat(rest.getForEntity("/api/clusters", String.class).getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void readShapedPostsStillWork() {
        var headers = new org.springframework.http.HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
        var response = rest.exchange("/api/clusters/does-not-exist/topics/some-topic/browse",
                HttpMethod.POST, new HttpEntity<>("{\"start\":\"latest\",\"limit\":1}", headers), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND); // past the read-only check
    }

    @Test
    void metaEndpointIsPublicAndReportsReadonly() {
        var response = rest.getForEntity("/api/meta", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"readonly\":true");
    }
}
