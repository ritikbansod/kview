package com.ritikbansod.kafkawrapper.search;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Whole-topic background search against an embedded broker: regex + contains
 * filters across partitions, limit, timestamp bounds and unknown ids.
 * One topic per test — searches see the full topic, so they must not share data.
 */
@SpringBootTest(properties = "kview.data-dir=target/search-test-data")
@EmbeddedKafka(partitions = 3, topics = {"search-regex", "search-limit", "search-ts"})
class SearchIntegrationTest {

    @Autowired
    SearchService searchService;

    @Autowired
    KafkaClusterManager clusterManager;

    @DynamicPropertySource
    static void kafkaBootstrap(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void regexSearchFindsMatchesAcrossAllPartitions() throws Exception {
        produce("search-regex", System.currentTimeMillis() - 30_000, 90);

        String id = searchService.start(KafkaClusterManager.DEFAULT_CLUSTER_ID, "search-regex",
                new SearchService.SearchRequest(null, null, null, null, "\"orderId\"\\s*:\\s*\"ORD-7[0-9]", null, null));
        SearchService.SearchStatus status = awaitDone(id);

        assertThat(status.state()).isEqualTo("DONE");
        // ORD-70..ORD-79 → 10 matches, spread over 3 partitions
        assertThat(status.matched()).isEqualTo(10);
        assertThat(status.partitionsTotal()).isEqualTo(3);
        assertThat(status.partitionsDone()).isEqualTo(3);
        assertThat(status.scanned()).isEqualTo(90);
        assertThat(status.results()).allSatisfy(m -> assertThat(m.value()).contains("ORD-7"));
        assertThat(status.results().get(0).partition()).isBetween(0, 2);
    }

    @Test
    void limitStopsTheScanEarly() throws Exception {
        produce("search-limit", System.currentTimeMillis() - 30_000, 90);

        String id = searchService.start(KafkaClusterManager.DEFAULT_CLUSTER_ID, "search-limit",
                new SearchService.SearchRequest(null, null, null, "amount", null, null, 5));
        SearchService.SearchStatus status = awaitDone(id);

        assertThat(status.state()).isEqualTo("DONE");
        assertThat(status.matched()).isEqualTo(5);
        assertThat(status.limitReached()).isTrue();
        assertThat(status.scanned()).isLessThan(90);
    }

    @Test
    void timestampBoundsNarrowTheScan() throws Exception {
        long base = System.currentTimeMillis() - 30_000;
        produce("search-ts", base, 90);

        String id = searchService.start(KafkaClusterManager.DEFAULT_CLUSTER_ID, "search-ts",
                new SearchService.SearchRequest(System.currentTimeMillis() + 60_000, null, null, null, null, null, null));
        SearchService.SearchStatus nothingAtOrAfter = awaitDone(id);
        assertThat(nothingAtOrAfter.state()).isEqualTo("DONE");
        assertThat(nothingAtOrAfter.matched()).isZero(); // every record is older than the fromTs bound

        String id2 = searchService.start(KafkaClusterManager.DEFAULT_CLUSTER_ID, "search-ts",
                new SearchService.SearchRequest(base - 1000, System.currentTimeMillis(), null, "amount", null, null, null));
        SearchService.SearchStatus all = awaitDone(id2);
        assertThat(all.matched()).isEqualTo(90);
    }

    @Test
    void unknownSearchIdIsReported() {
        assertThatThrownBy(() -> searchService.status(KafkaClusterManager.DEFAULT_CLUSTER_ID, "nope"))
                .isInstanceOf(NoSuchElementException.class);
    }

    private void produce(String topic, long baseTs, int count) throws Exception {
        var template = clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template();
        for (int i = 0; i < count; i++) {
            String payload = "{\"orderId\":\"ORD-" + i + "\",\"amount\":" + i + ".0}";
            template.send(new ProducerRecord<>(topic, i % 3, baseTs + i, ("k-" + i).getBytes(),
                    payload.getBytes())).get();
        }
    }

    private SearchService.SearchStatus awaitDone(String id) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        SearchService.SearchStatus status = searchService.status(KafkaClusterManager.DEFAULT_CLUSTER_ID, id);
        while ("RUNNING".equals(status.state()) && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            status = searchService.status(KafkaClusterManager.DEFAULT_CLUSTER_ID, id);
        }
        return status;
    }
}
