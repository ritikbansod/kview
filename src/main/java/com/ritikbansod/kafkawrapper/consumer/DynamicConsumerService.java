package com.ritikbansod.kafkawrapper.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers and removes message listener containers at runtime, so clients
 * can subscribe to a topic over REST without restarting anything.
 */
@Service
public class DynamicConsumerService {

    /** Bound of in-memory received messages kept per consumer group. */
    private static final int MAX_MESSAGES_PER_GROUP = 100;

    private final ConsumerFactory<String, String> consumerFactory;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> containers =
            new ConcurrentHashMap<>();
    private final Map<String, List<ReceivedMessage>> receivedByGroup = new ConcurrentHashMap<>();

    public DynamicConsumerService(ConsumerFactory<String, String> consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public Map<String, String> subscribe(String groupId, String topic) {
        String containerId = containerId(groupId, topic);
        if (containers.containsKey(containerId)) {
            return Map.of("consumer", containerId, "status", "already-running");
        }

        MessageListener<String, String> listener = record -> record(groupId, record);
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties(groupId, topic, listener));
        container.setBeanName(containerId);
        container.start();

        containers.put(containerId, container);
        receivedByGroup.put(groupId, receivedByGroup.getOrDefault(groupId, new ArrayList<>()));
        return Map.of("consumer", containerId, "status", "started");
    }

    public Map<String, String> unsubscribe(String groupId, String topic) {
        String containerId = containerId(groupId, topic);
        ConcurrentMessageListenerContainer<String, String> container = containers.remove(containerId);
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
        Map<String, Boolean> state = new ConcurrentHashMap<>();
        containers.forEach((id, container) -> state.put(id, container.isRunning()));
        return state;
    }

    private void record(String groupId, ConsumerRecord<String, String> record) {
        List<ReceivedMessage> messages = receivedByGroup.computeIfAbsent(groupId, k -> new ArrayList<>());
        synchronized (messages) {
            messages.add(new ReceivedMessage(record.topic(), record.key(), record.value(),
                    record.partition(), record.offset()));
            if (messages.size() > MAX_MESSAGES_PER_GROUP) {
                messages.removeFirst();
            }
        }
    }

    private ContainerProperties containerProperties(String groupId, String topic, MessageListener<String, String> listener) {
        ContainerProperties properties = new ContainerProperties(topic);
        properties.setGroupId(groupId);
        properties.setMessageListener(listener);
        properties.setMissingTopicsFatal(false);
        return properties;
    }

    private String containerId(String groupId, String topic) {
        return groupId + ":" + topic;
    }

    public record ReceivedMessage(String topic, String key, String value, int partition, long offset) {
    }
}
