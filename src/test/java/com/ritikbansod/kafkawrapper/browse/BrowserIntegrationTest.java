package com.ritikbansod.kafkawrapper.browse;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import com.ritikbansod.kafkawrapper.message.ProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test: produce through the wrapper, then read it back through the
 * seek-based browser — all against an in-memory Kafka broker.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = BrowserIntegrationTest.TOPIC)
class BrowserIntegrationTest {

    static final String TOPIC = "browser-test";

    @DynamicPropertySource
    static void kafkaBootstrap(DynamicPropertyRegistry registry) {
        // set by the EmbeddedKafka KRaft broker before the context refreshes
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }


    @Autowired
    MessageBrowserService browserService;

    @Autowired
    ProducerService producerService;

    @Autowired
    KafkaClusterManager clusterManager;

    @Test
    void producedMessagesAreBrowsedBack() throws Exception {
        for (int i = 0; i < 10; i++) {
            clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template()
                    .send(TOPIC, ("key-" + i).getBytes(), ("{\"n\":" + i + "}").getBytes()).get();
        }

        var result = browserService.browse(clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID),
                TOPIC, new MessageBrowserService.BrowseRequest(
                        null, "earliest", null, null, 100, 10_000L, null, null));

        assertThat(result.messages()).hasSize(10);
        assertThat(result.messages().get(0).offset()).isZero();
        assertThat(result.messages().get(9).value()).isEqualTo("{\"n\":9}");
        assertThat(result.reachedEnd()).isTrue();
    }

    @Test
    void valueFilterNarrowsResults() throws Exception {
        clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template()
                .send(TOPIC, "find-me".getBytes(), "payload-needle".getBytes()).get();
        clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID).template()
                .send(TOPIC, "other".getBytes(), "payload-haystack".getBytes()).get();

        var result = browserService.browse(clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID),
                TOPIC, new MessageBrowserService.BrowseRequest(
                        null, "earliest", null, null, 100, 10_000L, null, "needle"));

        assertThat(result.messages()).hasSize(1);
        assertThat(result.messages().get(0).key()).isEqualTo("find-me");
    }

    @Test
    void produceWithHeadersReturnsPosition() throws Exception {
        Map<String, Object> produced = producerService.produce(
                clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID),
                TOPIC, "hdr-key", "hdr-value", null, null, null, Map.of("source", "ui"));

        assertThat(produced.get("partition")).isEqualTo(0);
        assertThat((Long) produced.get("offset")).isGreaterThanOrEqualTo(0);

        var result = browserService.browse(clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID),
                TOPIC, new MessageBrowserService.BrowseRequest(
                        0, "earliest", null, null, 1000, 10_000L, "hdr-key", null));
        assertThat(result.messages()).anyMatch(m ->
                "hdr-value".equals(m.value()) && "ui".equals(m.headers().get("source")));
    }
}
