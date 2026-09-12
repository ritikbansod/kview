package com.ritikbansod.kafkawrapper.cluster;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

/**
 * One-shot cluster topology for the architecture view: brokers + controller,
 * every topic with its partition replica assignment and end offsets, and
 * consumer groups with member counts and the topics they read — the edges of
 * the diagram. ~5 admin calls total regardless of cluster size.
 */
@Service
public class ClusterTopologyService {

    private static final int TIMEOUT_MS = 10_000;

    public Topology topology(ClusterHandle handle) throws ExecutionException, InterruptedException {
        Admin admin = handle.admin();

        var cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(TIMEOUT_MS));
        int controllerId = cluster.controller().get() == null ? -1 : cluster.controller().get().id();
        List<BrokerNode> brokers = cluster.nodes().get().stream()
                .map(n -> new BrokerNode(n.id(), n.host(), n.port(), n.rack()))
                .sorted((a, b) -> Integer.compare(a.id(), b.id()))
                .toList();

        Collection<String> topicNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        admin.listTopics(new ListTopicsOptions().timeoutMs(TIMEOUT_MS).listInternal(true))
                .listings().get()
                .forEach(l -> topicNames.add(l.name()));

        Map<String, TopicDescription> descriptions = topicNames.isEmpty() ? Map.of()
                : admin.describeTopics(topicNames, new org.apache.kafka.clients.admin.DescribeTopicsOptions().timeoutMs(TIMEOUT_MS))
                        .allTopicNames().get();

        // end offsets for every partition of every topic in one call
        Map<TopicPartition, TopicPartitionInfo> tpInfos = new LinkedHashMap<>();
        descriptions.forEach((name, desc) -> desc.partitions()
                .forEach(p -> tpInfos.put(new TopicPartition(name, p.partition()), p)));
        Map<TopicPartition, Long> endOffsets = listEndOffsets(admin, tpInfos.keySet());

        List<TopicNode> topics = new ArrayList<>();
        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            TopicDescription desc = entry.getValue();
            List<PartitionNode> partitions = new ArrayList<>();
            long messageCount = 0;
            for (TopicPartitionInfo p : desc.partitions()) {
                long end = endOffsets.getOrDefault(new TopicPartition(desc.name(), p.partition()), -1L);
                if (end > 0) {
                    messageCount += end;
                }
                partitions.add(new PartitionNode(p.partition(),
                        p.leader() == null ? -1 : p.leader().id(),
                        p.replicas().stream().map(n -> n.id()).toList(),
                        p.isr().stream().map(n -> n.id()).toList(),
                        end));
            }
            partitions.sort((a, b) -> Integer.compare(a.partition(), b.partition()));
            short rf = partitions.isEmpty() ? 0 : (short) partitions.get(0).replicas().size();
            topics.add(new TopicNode(desc.name(), desc.isInternal(), rf, partitions, messageCount));
        }
        topics.sort((a, b) -> a.name().compareToIgnoreCase(b.name()));

        // consumer groups: member counts + consumed topics in two batched calls
        var listings = admin.listConsumerGroups(
                        new org.apache.kafka.clients.admin.ListConsumerGroupsOptions().timeoutMs(TIMEOUT_MS))
                .valid().get();
        List<String> groupIds = listings.stream().map(l -> l.groupId()).sorted().toList();

        Map<String, Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata>> offsetsByGroup = new HashMap<>();
        if (!groupIds.isEmpty()) {
            Map<String, ListConsumerGroupOffsetsSpec> specs = new HashMap<>();
            groupIds.forEach(g -> specs.put(g, new ListConsumerGroupOffsetsSpec()));
            try {
                offsetsByGroup.putAll(admin.listConsumerGroupOffsets(specs,
                                new ListConsumerGroupOffsetsOptions().timeoutMs(TIMEOUT_MS)).all().get());
            } catch (ExecutionException e) {
                // best-effort: groups still render without lag/edges
            }
        }

        Set<TopicPartition> committedTps = new java.util.HashSet<>();
        offsetsByGroup.values().forEach(m -> committedTps.addAll(m.keySet()));
        Map<TopicPartition, Long> groupEndOffsets = listEndOffsets(admin, committedTps);

        Map<String, ConsumerGroupDescription> groupDescriptions = groupIds.isEmpty() ? Map.of()
                : admin.describeConsumerGroups(groupIds, new DescribeConsumerGroupsOptions().timeoutMs(TIMEOUT_MS))
                        .all().get();

        List<GroupNode> groups = new ArrayList<>();
        for (String groupId : groupIds) {
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
                    offsetsByGroup.getOrDefault(groupId, Map.of());
            Set<String> readTopics = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            offsets.keySet().forEach(tp -> readTopics.add(tp.topic()));
            final long[] lagHolder = {0};
            offsets.forEach((tp, om) -> {
                Long end = groupEndOffsets.get(tp);
                if (end != null) {
                    lagHolder[0] += Math.max(0, end - om.offset());
                }
            });
            long totalLag = lagHolder[0];

            ConsumerGroupDescription desc = groupDescriptions.get(groupId);
            int members = desc == null ? 0 : desc.members().size();
            int coordinator = desc == null || desc.coordinator() == null ? -1 : desc.coordinator().id();
            String state = desc == null ? "UNKNOWN" : desc.state().name();
            groups.add(new GroupNode(groupId, state, members, coordinator, totalLag,
                    new ArrayList<>(readTopics), offsets.size()));
        }
        groups.sort((a, b) -> a.groupId().compareToIgnoreCase(b.groupId()));

        return new Topology(brokers, controllerId, topics, groups);
    }

    private Map<TopicPartition, Long> listEndOffsets(Admin admin, Collection<TopicPartition> tps)
            throws InterruptedException, ExecutionException {
        Map<TopicPartition, Long> ends = new HashMap<>();
        if (tps.isEmpty()) {
            return ends;
        }
        Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
        tps.forEach(tp -> specs.put(tp, OffsetSpec.latest()));
        try {
            admin.listOffsets(specs, new ListOffsetsOptions().timeoutMs(TIMEOUT_MS)).all().get()
                    .forEach((tp, info) -> ends.put(tp, info.offset()));
        } catch (ExecutionException e) {
            // offsets unavailable — partitions render with endOffset -1
        }
        return ends;
    }

    public record BrokerNode(int id, String host, int port, String rack) {
    }

    public record PartitionNode(int partition, int leader, List<Integer> replicas, List<Integer> isr, long endOffset) {
    }

    public record TopicNode(String name, boolean internal, short replicationFactor,
                            List<PartitionNode> partitions, long messageCount) {
    }

    public record GroupNode(String groupId, String state, int members, int coordinatorId,
                            long totalLag, List<String> topics, int committedPartitions) {
    }

    public record Topology(List<BrokerNode> brokers, int controller, List<TopicNode> topics, List<GroupNode> groups) {
    }
}
