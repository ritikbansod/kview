package com.ritikbansod.kafkawrapper.topic;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Full topic introspection + lifecycle over the AdminClient, scoped to a connection:
 * metadata, per-partition offsets, configs, create/resize/reconfigure/delete.
 */
@Service
public class TopicManagementService {

    private static final int TIMEOUT_MS = 10_000;

    public List<TopicSummary> list(ClusterHandle handle, boolean includeCounts)
            throws ExecutionException, InterruptedException {
        var admin = handle.admin();
        java.util.Set<String> names = admin.listTopics(new org.apache.kafka.clients.admin.ListTopicsOptions()
                        .listInternal(true).timeoutMs(TIMEOUT_MS)).names().get();
        Map<String, TopicDescription> descriptions = admin.describeTopics(names,
                        new org.apache.kafka.clients.admin.DescribeTopicsOptions().timeoutMs(TIMEOUT_MS))
                .allTopicNames().get();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        if (includeCounts) {
            for (TopicDescription description : descriptions.values()) {
                for (TopicPartitionInfo p : description.partitions()) {
                    TopicPartition tp = new TopicPartition(description.name(), p.partition());
                    endOffsets.put(tp, 0L);
                    beginningOffsets.put(tp, 0L);
                }
            }
            try {
                admin.listOffsets(specs(endOffsets.keySet(), OffsetSpec.latest())).all().get()
                        .forEach((tp, info) -> endOffsets.put(tp, info.offset()));
                admin.listOffsets(specs(beginningOffsets.keySet(), OffsetSpec.earliest())).all().get()
                        .forEach((tp, info) -> beginningOffsets.put(tp, info.offset()));
            } catch (ExecutionException e) {
                // offsets are best-effort on the list view; keep zeros on failure
            }
        }

        List<TopicSummary> result = new ArrayList<>();
        for (TopicDescription description : descriptions.values()) {
            int underReplicated = (int) description.partitions().stream()
                    .filter(p -> p.isr().size() < p.replicas().size()).count();
            long count = 0;
            for (TopicPartitionInfo p : description.partitions()) {
                TopicPartition tp = new TopicPartition(description.name(), p.partition());
                count += Math.max(0, endOffsets.getOrDefault(tp, 0L) - beginningOffsets.getOrDefault(tp, 0L));
            }
            result.add(new TopicSummary(description.name(), description.isInternal(), description.partitions().size(),
                    description.partitions().isEmpty() ? 0
                            : (short) description.partitions().get(0).replicas().size(),
                    count, underReplicated));
        }
        result.sort((a, b) -> a.name().compareToIgnoreCase(b.name()));
        return result;
    }

    public TopicDetail detail(ClusterHandle handle, String topic)
            throws ExecutionException, InterruptedException {
        var admin = handle.admin();
        TopicDescription description = admin.describeTopics(List.of(topic))
                .allTopicNames().get().get(topic);
        if (description == null) {
            throw new org.apache.kafka.common.errors.UnknownTopicOrPartitionException(topic);
        }

        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Config config = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        List<Map<String, Object>> configView = new ArrayList<>();
        for (ConfigEntry entry : config.entries()) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("name", entry.name());
            view.put("value", entry.isSensitive() ? "••••••" : entry.value());
            view.put("source", entry.source().name());
            view.put("sensitive", entry.isSensitive());
            view.put("readOnly", entry.isReadOnly());
            view.put("documentation", entry.documentation());
            configView.add(view);
        }

        Map<TopicPartition, TopicPartitionInfo> infos = new HashMap<>();
        for (TopicPartitionInfo p : description.partitions()) {
            infos.put(new TopicPartition(topic, p.partition()), p);
        }
        Map<TopicPartition, ListOffsetsResultInfo> ends = admin
                .listOffsets(specs(infos.keySet(), OffsetSpec.latest())).all().get();
        Map<TopicPartition, ListOffsetsResultInfo> beginnings = admin
                .listOffsets(specs(infos.keySet(), OffsetSpec.earliest())).all().get();

        List<PartitionDetail> partitions = new ArrayList<>();
        for (TopicPartitionInfo p : description.partitions()) {
            TopicPartition tp = new TopicPartition(topic, p.partition());
            long end = ends.containsKey(tp) ? ends.get(tp).offset() : -1;
            long beginning = beginnings.containsKey(tp) ? beginnings.get(tp).offset() : -1;
            partitions.add(new PartitionDetail(p.partition(),
                    p.leader() == null ? -1 : p.leader().id(),
                    ids(p.replicas()), ids(p.isr()),
                    beginning, end, Math.max(0, end - beginning)));
        }
        partitions.sort((a, b) -> Integer.compare(a.partition(), b.partition()));
        return new TopicDetail(description.name(), description.topicId() == null ? null
                : description.topicId().toString(), description.isInternal(), partitions, configView);
    }

    public Map<String, Object> create(ClusterHandle handle, String name, int partitions, short replicationFactor,
                                      Map<String, String> configs) throws ExecutionException, InterruptedException {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            NewTopic newTopic = new NewTopic(name, partitions, replicationFactor);
            if (configs != null && !configs.isEmpty()) {
                newTopic.configs(configs);
            }
            handle.admin().createTopics(List.of(newTopic)).all().get();
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

    public Map<String, Object> increasePartitions(ClusterHandle handle, String topic, int totalPartitions)
            throws ExecutionException, InterruptedException {
        handle.admin().createPartitions(Map.of(topic, NewPartitions.increaseTo(totalPartitions))).all().get();
        return Map.of("topic", topic, "partitions", totalPartitions, "status", "altered");
    }

    public Map<String, Object> alterConfigs(ClusterHandle handle, String topic, Map<String, String> configs)
            throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        configs.forEach((key, value) -> ops.add(new AlterConfigOp(
                new ConfigEntry(key, value), AlterConfigOp.OpType.SET)));
        handle.admin().incrementalAlterConfigs(Map.of(resource, ops)).all().get();
        return Map.of("topic", topic, "status", "altered", "configs", configs);
    }

    public Map<String, Object> delete(ClusterHandle handle, String topic) throws ExecutionException, InterruptedException {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            handle.admin().deleteTopics(List.of(topic)).all().get();
            result.put("topic", topic);
            result.put("status", "deleted");
        } catch (ExecutionException e) {
            result.put("topic", topic);
            result.put("status", "delete-failed");
            result.put("reason", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
        }
        return result;
    }

    private static Map<TopicPartition, OffsetSpec> specs(Collection<TopicPartition> tps, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> map = new HashMap<>();
        tps.forEach(tp -> map.put(tp, spec));
        return map;
    }

    private static List<Integer> ids(List<Node> nodes) {
        return nodes.stream().map(Node::id).toList();
    }

    public record TopicSummary(String name, boolean internal, int partitions, short replicationFactor,
                               long messageCount, int underReplicatedPartitions) {
    }

    public record PartitionDetail(int partition, int leader, List<Integer> replicas, List<Integer> isr,
                                  long beginningOffset, long endOffset, long messageCount) {
    }

    public record TopicDetail(String name, String topicId, boolean internal,
                              List<PartitionDetail> partitions, List<Map<String, Object>> configs) {
    }
}
