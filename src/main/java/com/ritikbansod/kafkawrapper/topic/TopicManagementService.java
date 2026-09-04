package com.ritikbansod.kafkawrapper.topic;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Wraps the Kafka AdminClient so clients manage topics over REST
 * instead of the Kafka CLI.
 */
@Service
public class TopicManagementService {

    private final Admin admin;

    public TopicManagementService(Admin admin) {
        this.admin = admin;
    }

    public Map<String, Object> create(String name, int partitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        Map<String, Object> result = new HashMap<>();
        try {
            admin.createTopics(java.util.List.of(new NewTopic(name, partitions, replicationFactor)))
                    .all().get();
            result.put("topic", name);
            result.put("status", "created");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                result.put("topic", name);
                result.put("status", "already-exists");
            } else {
                throw e;
            }
        }
        return result;
    }

    public Set<String> list() throws ExecutionException, InterruptedException {
        return admin.listTopics().names().get();
    }

    public Map<String, String> describe(String name) throws ExecutionException, InterruptedException {
        return admin.describeTopics(java.util.List.of(name))
                .allTopicNames().get()
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
    }

    public Map<String, String> delete(String name) throws ExecutionException, InterruptedException {
        Map<String, String> result = new HashMap<>();
        try {
            admin.deleteTopics(java.util.List.of(name)).all().get();
            result.put("topic", name);
            result.put("status", "deleted");
        } catch (ExecutionException e) {
            result.put("topic", name);
            result.put("status", "delete-failed");
            result.put("reason", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        }
        return result;
    }
}
