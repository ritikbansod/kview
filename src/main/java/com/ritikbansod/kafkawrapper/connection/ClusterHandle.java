package com.ritikbansod.kafkawrapper.connection;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Map;

/**
 * All live Kafka clients for a single cluster connection: Admin, Producer, Consumer.
 * Created by {@link KafkaClusterManager} and closed on reconnect/delete.
 */
public final class ClusterHandle implements AutoCloseable {

    private final ConnectionProfile profile;
    private final boolean builtIn;
    private final Admin admin;
    private final KafkaTemplate<byte[], byte[]> template;
    private final Map<String, Object> consumerProperties;
    private final DefaultKafkaConsumerFactory<byte[], byte[]> consumerFactory;

    public ClusterHandle(ConnectionProfile profile, boolean builtIn, Admin admin,
                         KafkaTemplate<byte[], byte[]> template,
                         Map<String, Object> consumerProperties) {
        this.profile = profile;
        this.builtIn = builtIn;
        this.admin = admin;
        this.template = template;
        this.consumerProperties = consumerProperties;
        this.consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    public ConnectionProfile profile() {
        return profile;
    }

    public boolean builtIn() {
        return builtIn;
    }

    public Admin admin() {
        return admin;
    }

    public KafkaTemplate<byte[], byte[]> template() {
        return template;
    }

    /** Consumer config of this connection — used to spin up tail/browser consumers. */
    public Map<String, Object> consumerProperties() {
        return consumerProperties;
    }

    /** Spring consumer factory for listener containers built from this connection's config. */
    public DefaultKafkaConsumerFactory<byte[], byte[]> springConsumerFactory() {
        return consumerFactory;
    }

    /** Fresh consumer built from the connection's config (caller owns and must close it). */
    public KafkaConsumer<byte[], byte[]> createConsumer() {
        return new KafkaConsumer<>(consumerProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    /** Fresh raw producer built from the connection's config (caller owns and must close it). */
    public KafkaProducer<byte[], byte[]> createProducer() {
        return new KafkaProducer<>(template.getProducerFactory().getConfigurationProperties(),
                new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Override
    public void close() {
        try {
            admin.close(Duration.ofSeconds(5));
        } catch (RuntimeException e) {
            // closing a dead client must never break control flow
        }
        try {
            if (template.getProducerFactory() instanceof org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> pf) {
                pf.destroy();
            }
        } catch (RuntimeException e) {
            // idem
        }
    }
}
