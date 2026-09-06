package com.ritikbansod.kafkawrapper.consumer;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers and removes message listener containers at runtime, so clients
 * can subscribe to a topic over REST without restarting anything and read
 * what a group receives — useful to verify a pipeline end to end.
 */
@Service
public class DynamicConsumerService {

    /** Upper bound of in-memory received messages kept per consumer group. */
    private static final int MAX_MESSAGES_PER_GROUP = 100;

    private final KafkaClusterManager manager;
    private final Map<String, ConcurrentMessageListenerContainer<byte[], byte[]>> containers =
            new ConcurrentHashMap<>();
    private final Map<String, List<ReceivedMessage>> receivedByGroup = new ConcurrentHashMap<>();

    public DynamicConsumerService(KafkaClusterManager manager) {
        this.manager = manager;
    }

    public Map<String, String> subscribe(String clusterId, String groupId, String topic) {
        String containerId = containerId(clusterId, groupId, topic);
        if (containers.containsKey(containerId)) {
            return Map.of("consumer", containerId, "status", "already-running");
        }

        ClusterHandle handle = manager.get(clusterId);
        MessageListener<byte[], byte[]> listener = record -> record(clusterId, groupId, record);
        ContainerProperties properties = new ContainerProperties(topic);
        properties.setGroupId(groupId);
        properties.setMessageListener(listener);
        properties.setMissingTopicsFatal(false);

        ConcurrentMessageListenerContainer<byte[], byte[]> container =
                new ConcurrentMessageListenerContainer<>(handle.springConsumerFactory(), properties);
        container.setBeanName(containerId);
        container.start();

        containers.put(containerId, container);
        receivedByGroup.putIfAbsent(groupId, new ArrayList<>());
        return Map.of("consumer", containerId, "status", "started");
    }

    public Map<String, String> unsubscribe(String clusterId, String groupId, String topic) {
        String containerId = containerId(clusterId, groupId, topic);
        ConcurrentMessageListenerContainer<byte[], byte[]> container = containers.remove(containerId);
        if (container == null) {
            return Map.of("consumer", containerId, "status", "not-running");
        }
        container.stop();
        return Map.of("consumer", containerId, "status", "stopped");
    }

    public List<ReceivedMessage> received(String groupId) {
        return List.copyOf(receivedByGroup.getOrDefault(groupId, List.of()));
    }

    public Map<String, Boolean> running() {
        Map<String, Boolean> state = new LinkedHashMap<>();
        containers.forEach((id, container) -> state.put(id, container.isRunning()));
        return state;
    }

    private void record(String clusterId, String groupId, ConsumerRecord<byte[], byte[]> record) {
        List<ReceivedMessage> messages = receivedByGroup.computeIfAbsent(groupId, k -> new ArrayList<>());
        Map<String, String> headers = new LinkedHashMap<>();
        record.headers().forEach(h -> headers.put(h.key(),
                h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8)));
        synchronized (messages) {
            messages.add(new ReceivedMessage(clusterId, record.topic(),
                    asText(record.key()), asText(record.value()),
                    record.partition(), record.offset(), record.timestamp(), headers));
            if (messages.size() > MAX_MESSAGES_PER_GROUP) {
                messages.removeFirst();
            }
        }
    }

    private static String asText(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private String containerId(String clusterId, String groupId, String topic) {
        return clusterId + ":" + groupId + ":" + topic;
    }

    public record ReceivedMessage(String clusterId, String topic, String key, String value,
                                  int partition, long offset, long timestamp, Map<String, String> headers) {
    }
}
