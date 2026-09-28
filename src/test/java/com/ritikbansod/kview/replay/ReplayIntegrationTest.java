package com.ritikbansod.kview.replay;

import com.ritikbansod.kview.browse.MessageBrowserService;
import com.ritikbansod.kview.connection.ConnectionProfile;
import com.ritikbansod.kview.connection.ConnectionStore;
import com.ritikbansod.kview.connection.KafkaClusterManager;
import com.ritikbansod.kview.connection.SecuritySettings;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bulk replay against an embedded broker: same-cluster copy with header and
 * timestamp preservation, dry-run + filters, and a cross-connection replay.
 * One source topic per test — the selection reads the whole source topic.
 */
@SpringBootTest(properties = "kview.data-dir=target/replay-test-data")
@EmbeddedKafka(partitions = 3, topics = {"replay-src-a", "replay-src-b", "replay-src-c",
        "replay-dst-a", "replay-dst-b", "replay-cross"})
class ReplayIntegrationTest {

    @Autowired
    ReplayService replayService;

    @Autowired
    MessageBrowserService browser;

    @Autowired
    KafkaClusterManager clusterManager;

    @Autowired
    ConnectionStore connectionStore;

    @DynamicPropertySource
    static void kafkaBootstrap(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void sameClusterReplayCopiesKeysHeadersAndTimestamps() throws Exception {
        long base = System.currentTimeMillis() - 30_000;
        produce("replay-src-a", base, 20);

        var result = replayService.replay(KafkaClusterManager.DEFAULT_CLUSTER_ID, "replay-src-a",
                new ReplayService.ReplayRequest(null, "replay-dst-a", true, false, null, null, null));

        assertThat(result.copied()).isEqualTo(20);
        assertThat(result.errors()).isEmpty();
        assertThat(result.reachedEnd()).isTrue();

        var dst = browseAll("replay-dst-a");
        assertThat(dst.messages()).hasSize(20);
        MessageBrowserService.BrowserMessage k0 = dst.messages().stream()
                .filter(m -> "k-0".equals(m.key())).findFirst().orElseThrow();
        assertThat(k0.timestamp()).isEqualTo(base); // original timestamp preserved
        assertThat(k0.headers()).containsEntry("retry", "3");
        assertThat(k0.value()).contains("\"note\":\"poison\"");
        assertThat(dst.messages()).extracting(MessageBrowserService.BrowserMessage::key)
                .containsExactlyInAnyOrderElementsOf(java.util.stream.IntStream.range(0, 20)
                        .mapToObj(i -> "k-" + i).toList());
    }

    @Test
    void dryRunSelectsWithoutProducingAndFiltersNarrow() throws Exception {
        produce("replay-src-b", System.currentTimeMillis() - 30_000, 20);

        var dry = replayService.replay(KafkaClusterManager.DEFAULT_CLUSTER_ID, "replay-src-b",
                new ReplayService.ReplayRequest(null, "replay-dst-b", true, true, null, null, "poison"));
        assertThat(dry.dryRun()).isTrue();
        assertThat(dry.selected()).isEqualTo(4); // i % 5 == 0 → ORD-0,5,10,15 carry "poison"
        assertThat(dry.copied()).isZero();

        var filtered = replayService.replay(KafkaClusterManager.DEFAULT_CLUSTER_ID, "replay-src-b",
                new ReplayService.ReplayRequest(null, "replay-dst-b", true, false, null, null, "poison"));
        assertThat(filtered.copied()).isEqualTo(4);
        assertThat(browseAll("replay-dst-b").messages()).hasSize(4);
    }

    @Test
    void replayCanCrossToAnotherConnection() throws Exception {
        produce("replay-src-c", System.currentTimeMillis() - 30_000, 20);
        connectionStore.save(new ConnectionProfile("second", "Second (same broker)",
                List.of(System.getProperty("spring.embedded.kafka.brokers").split(",")[0]),
                SecuritySettings.plaintext()));
        clusterManager.evict("second"); // pick up the freshly saved profile

        var result = replayService.replay(KafkaClusterManager.DEFAULT_CLUSTER_ID, "replay-src-c",
                new ReplayService.ReplayRequest("second", "replay-cross", false, false, null, null, null));

        assertThat(result.copied()).isEqualTo(20);
        assertThat(result.targetClusterId()).isEqualTo("second");
        var viaSecond = browser.browse(clusterManager.get("second"), "replay-cross",
                new MessageBrowserService.BrowseRequest(null, "earliest", null, null, 100, 10_000L, null, null));
        assertThat(viaSecond.messages()).hasSize(20);
    }

    private void produce(String topic, long baseTs, int count) throws Exception {
        var template = clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template();
        for (int i = 0; i < count; i++) {
            String payload = "{\"orderId\":\"ORD-" + i + "\",\"note\":\"" + (i % 5 == 0 ? "poison" : "fine") + "\"}";
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, i % 3, baseTs + i,
                    ("k-" + i).getBytes(), payload.getBytes());
            if (i < 5) record.headers().add("retry", "3".getBytes());
            template.send(record).get();
        }
    }

    private MessageBrowserService.BrowseResult browseAll(String topic) {
        return browser.browse(clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID), topic,
                new MessageBrowserService.BrowseRequest(null, "earliest", null, null, 100, 10_000L, null, null));
    }
}
