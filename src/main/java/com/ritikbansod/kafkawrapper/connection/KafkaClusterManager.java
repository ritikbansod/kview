package com.ritikbansod.kafkawrapper.connection;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.springframework.kafka.core.KafkaTemplate;

/**
 * Owns one {@link ClusterHandle} per connection, created on first use and cached.
 * The built-in "default" cluster comes from spring.kafka.* (application.yml) so the
 * pre-existing API keeps working without any stored profile.
 */
@Service
public class KafkaClusterManager {

    public static final String DEFAULT_CLUSTER_ID = "default";

    private static final Logger log = LoggerFactory.getLogger(KafkaClusterManager.class);
    private static final int TEST_TIMEOUT_MS = 10_000;

    private final ConnectionStore store;
    private final KafkaProperties springKafkaProperties;
    private final Map<String, ClusterHandle> handles = new ConcurrentHashMap<>();

    public KafkaClusterManager(ConnectionStore store, KafkaProperties springKafkaProperties) {
        this.store = store;
        this.springKafkaProperties = springKafkaProperties;
    }

    public ConnectionProfile profileOf(String clusterId) {
        if (DEFAULT_CLUSTER_ID.equals(clusterId)) {
            return builtInProfile();
        }
        return store.find(clusterId)
                .orElseThrow(() -> new NoSuchElementException("Unknown cluster id '" + clusterId + "'"));
    }

    public ClusterHandle get(String clusterId) {
        if (DEFAULT_CLUSTER_ID.equals(clusterId)) {
            return getOrCreateDefault();
        }
        ClusterHandle existing = handles.get(clusterId);
        if (existing != null) {
            return existing;
        }
        ConnectionProfile profile = profileOf(clusterId); // throws NoSuchElement for unknown ids
        try {
            return handles.computeIfAbsent(clusterId, id -> createHandle(profile, false));
        } catch (RuntimeException e) {
            throw new IllegalStateException("Could not connect to cluster '" + profile.displayName()
                    + "' (" + String.join(",", profile.bootstrapServers()) + "): " + rootMessage(e), e);
        }
    }

    private synchronized ClusterHandle getOrCreateDefault() {
        ClusterHandle existing = handles.get(DEFAULT_CLUSTER_ID);
        if (existing != null) {
            return existing;
        }
        try {
            ClusterHandle handle = createHandle(builtInProfile(), true);
            handles.put(DEFAULT_CLUSTER_ID, handle);
            return handle;
        } catch (RuntimeException e) {
            throw new IllegalStateException("Could not create the built-in Kafka client: " + rootMessage(e), e);
        }
    }

    /** Drops cached clients so the next access reconnects with current profile settings. */
    public void evict(String clusterId) {
        ClusterHandle handle = handles.remove(clusterId);
        if (handle != null) {
            log.info("Closed clients for cluster '{}'", clusterId);
            handle.close();
        }
    }

    public TestResult test(ConnectionProfile profile) {
        Map<String, Object> props = KafkaClientPropertiesFactory.create(profile);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, TEST_TIMEOUT_MS);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, TEST_TIMEOUT_MS);
        try (Admin admin = AdminClient.create(props)) {
            DescribeClusterResult result = admin.describeCluster(new DescribeClusterOptions().timeoutMs(TEST_TIMEOUT_MS));
            Collection<Node> nodes = result.nodes().get();
            String clusterId = com.ritikbansod.kafkawrapper.cluster.ClusterService.sanitizeClusterId(result.clusterId().get());
            List<Map<String, Object>> nodeViews = nodes.stream()
                    .map(n -> {
                        Map<String, Object> view = new LinkedHashMap<>();
                        view.put("id", n.id());
                        view.put("host", n.host());
                        view.put("port", n.port());
                        return view;
                    })
                    .toList();
            return new TestResult(true, clusterId, nodeViews, null);
        } catch (Exception e) {
            return new TestResult(false, null, List.of(), rootMessage(e));
        }
    }

    private ClusterHandle createHandle(ConnectionProfile profile, boolean builtIn) {
        Map<String, Object> common = KafkaClientPropertiesFactory.create(profile);
        Map<String, Object> adminProps = new HashMap<>(common);
        Map<String, Object> producerProps = new HashMap<>(common);
        Map<String, Object> consumerProps = new HashMap<>(common);
        // raw byte pipeline: the wrapper must see schema-registry payloads unharmed
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("auto.offset.reset", "earliest");

        if (builtIn) {
            adminProps = springKafkaProperties.buildAdminProperties();
            producerProps = springKafkaProperties.buildProducerProperties();
            consumerProps = springKafkaProperties.buildConsumerProperties();
            // byte pipeline wins over yml-configured String serdes (raw fidelity for SR payloads)
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        }

        // Fail fast when a cluster is unreachable instead of hanging for the 60s default
        adminProps.putIfAbsent(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        adminProps.putIfAbsent(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        consumerProps.putIfAbsent(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        consumerProps.putIfAbsent(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        producerProps.putIfAbsent(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        // the wrapper manages topics explicitly — never silently auto-create them
        consumerProps.putIfAbsent(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        Admin admin = AdminClient.create(adminProps);
        org.springframework.kafka.core.DefaultKafkaProducerFactory<byte[], byte[]> producerFactory =
                new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<byte[], byte[]> template = new KafkaTemplate<>(producerFactory);
        log.info("Created Kafka clients for cluster '{}' [{}]", profile.displayName(),
                profile.security() == null ? SecuritySettings.PLAINTEXT : profile.security().protocolOrDefault());
        return new ClusterHandle(profile, builtIn, admin, template, consumerProps);
    }

    private ConnectionProfile builtInProfile() {
        List<String> servers = springKafkaProperties.getBootstrapServers();
        return new ConnectionProfile(DEFAULT_CLUSTER_ID, "Default (application.yml)",
                servers, SecuritySettings.plaintext());
    }

    private static String rootMessage(Throwable e) {
        if (e instanceof ExecutionException && e.getCause() != null) {
            return rootMessage(e.getCause());
        }
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }

    public record TestResult(boolean ok, String clusterId, List<Map<String, Object>> nodes, String error) {
    }
}
