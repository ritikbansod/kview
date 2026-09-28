package com.ritikbansod.kview.metrics;

import com.ritikbansod.kview.connection.KafkaClusterManager;
import com.ritikbansod.kview.message.ProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Prometheus endpoint: a sample pass against the embedded broker publishes
 * the kview_* cluster series, and produce increments the produced counter.
 * {@code @AutoConfigureObservability} re-enables metrics export, which test
 * contexts disable by default.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "kview.data-dir=target/metrics-test-data")
@org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
@EmbeddedKafka(partitions = 1, topics = "metrics-test-topic")
class MetricsEndpointTest {

    @Autowired
    MetricsSampler sampler;

    @Autowired
    ProducerService producerService;

    @Autowired
    KafkaClusterManager clusterManager;

    @Autowired
    TestRestTemplate rest;

    @DynamicPropertySource
    static void kafkaBootstrap(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void prometheusEndpointExposesKviewSeries() throws Exception {
        producerService.produce(clusterManager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID),
                "metrics-test-topic", "k", "{\"n\":1}", null, null, null, Map.of());
        sampler.sample();

        var response = rest.getForEntity("/actuator/prometheus", String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody())
                .contains("kview_cluster_topics")
                .contains("kview_cluster_partitions")
                .contains("kview_cluster_under_replicated_partitions")
                .contains("kview_cluster_offline_partitions")
                .contains("kview_cluster_reachable")
                .contains("kview_messages_produced_total")
                .contains("cluster=\"default\"");
    }

    @Test
    void endpointIsPublicInDefaultMode() {
        assertThat(rest.getForEntity("/actuator/prometheus", String.class).getStatusCode())
                .isEqualTo(HttpStatus.OK);
    }
}
