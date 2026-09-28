package com.ritikbansod.kafkawrapper.auth;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The audit trail ({@code KVIEW_AUDIT=true}): one JSONL line per mutation with
 * method, path (cluster + topic), status and client; reads are not audited.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "kview.data-dir=target/audit-test-data",
        "kview.audit=true"
})
class AuditLogTest {

    @Autowired
    TestRestTemplate rest;

    private static final Path AUDIT = Path.of("target/audit-test-data/audit.log");

    @Test
    void mutationsAreAuditedWithMethodPathStatusAndClient() throws Exception {
        var headers = new org.springframework.http.HttpHeaders();
        headers.set("X-Kview-Client", "kview-cli/test");
        rest.exchange("/api/clusters/does-not-exist/topics/some-topic",
                HttpMethod.DELETE, new HttpEntity<Void>(headers), String.class);

        assertThat(AUDIT).exists();
        String line = Files.readString(AUDIT).trim();
        assertThat(line).contains("\"method\":\"DELETE\"");
        assertThat(line).contains("/api/clusters/does-not-exist/topics/some-topic");
        assertThat(line).contains("\"status\":404");
        assertThat(line).contains("\"client\":\"kview-cli/test\"");
    }

    @Test
    void readsAreNotAudited() throws Exception {
        int linesBefore = Files.exists(AUDIT) ? Files.readAllLines(AUDIT).size() : 0;
        rest.getForEntity("/api/clusters", String.class);
        int linesAfter = Files.exists(AUDIT) ? Files.readAllLines(AUDIT).size() : 0;
        assertThat(linesAfter).isEqualTo(linesBefore);
    }
}
