package com.ritikbansod.kafkawrapper.group;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
/**
 * Consumer group introspection: groups with state, member assignments,
 * committed vs end offsets and per-partition lag, offset reset, delete.
 */
@Service
public class ConsumerGroupService {

    private static final int TIMEOUT_MS = 10_000;

    public List<GroupSummary> list(ClusterHandle handle) throws ExecutionException, InterruptedException {
        var admin = handle.admin();
        List<ConsumerGroupListing> listings = new ArrayList<>(admin.listConsumerGroups(
                        new org.apache.kafka.clients.admin.ListConsumerGroupsOptions().timeoutMs(TIMEOUT_MS))
                .valid().get());

        Map<String, org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec> specsByGroup = new HashMap<>();
        listings.forEach(l -> specsByGroup.put(l.groupId(), new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec()));
        Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsByGroup = new HashMap<>();
        try {
            offsetsByGroup = admin.listConsumerGroupOffsets(specsByGroup,
                            new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions().timeoutMs(TIMEOUT_MS))
                    .all().get();
        } catch (ExecutionException e) {
            // best-effort: show groups even if offsets listing fails
        }

        Map<TopicPartition, ListOffsetsResultInfo> endOffsets = new HashMap<>();
        try {
            Set<TopicPartition> allTps = new java.util.HashSet<>();
            offsetsByGroup.values().forEach(m -> allTps.addAll(m.keySet()));
            if (!allTps.isEmpty()) {
                endOffsets = admin.listOffsets(specs(allTps, OffsetSpec.latest())).all().get();
            }
        } catch (ExecutionException e) {
            // idem
        }

        List<GroupSummary> groups = new ArrayList<>();
        for (ConsumerGroupListing listing : listings) {
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    offsetsByGroup.getOrDefault(listing.groupId(), Map.of());
            long totalLag = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
                ListOffsetsResultInfo end = endOffsets.get(e.getKey());
                if (end != null) {
                    totalLag += Math.max(0, end.offset() - e.getValue().offset());
                }
            }
            groups.add(new GroupSummary(listing.groupId(),
                    listing.state().map(Enum::name).orElse("UNKNOWN"),
                    offsets.size(), totalLag));
        }
        groups.sort((a, b) -> a.groupId().compareToIgnoreCase(b.groupId()));
        return groups;
    }

    public GroupDetail detail(ClusterHandle handle, String groupId) throws ExecutionException, InterruptedException {
        var admin = handle.admin();
        ConsumerGroupDescription description = admin.describeConsumerGroups(List.of(groupId),
                        new org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions().timeoutMs(TIMEOUT_MS))
                .all().get().get(groupId);
        if (description == null) {
            throw new GroupIdNotFoundException(groupId);
        }

        // TopicPartition is not Comparable on newer Kafka clients — do not use a TreeMap here
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        try {
            offsets.putAll(admin.listConsumerGroupOffsets(groupId,
                            new org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions().timeoutMs(TIMEOUT_MS))
                    .partitionsToOffsetAndMetadata().get());
        } catch (ExecutionException e) {
            // group may have no committed offsets yet
        }

        Map<TopicPartition, ListOffsetsResultInfo> ends = new HashMap<>();
        if (!offsets.isEmpty()) {
            try {
                ends = admin.listOffsets(specs(offsets.keySet(), OffsetSpec.latest())).all().get();
            } catch (ExecutionException e) {
                // lag shown as unknown
            }
        }

        Map<String, List<PartitionLag>> byTopic = new TreeMap<>();
        long totalLag = 0;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committed = entry.getValue().offset();
            Long end = ends.containsKey(tp) ? ends.get(tp).offset() : null;
            long lag = end == null ? -1 : Math.max(0, end - committed);
            if (lag >= 0) {
                totalLag += lag;
            }
            byTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>())
                    .add(new PartitionLag(tp.partition(), committed, end, lag));
        }
        byTopic.values().forEach(l -> l.sort((a, b) -> Integer.compare(a.partition(), b.partition())));

        List<MemberView> members = description.members().stream()
                .map(m -> new MemberView(m.consumerId(), m.clientId(), m.host(),
                        new ArrayList<>(m.assignment().topicPartitions()).stream()
                                .map(TopicPartition::toString).sorted().toList()))
                .toList();

        return new GroupDetail(groupId, description.state().name(),
                description.coordinator() == null ? null : description.coordinator().id(),
                members, byTopic, totalLag, offsets.size());
    }

    public Map<String, Object> delete(ClusterHandle handle, String groupId) throws ExecutionException, InterruptedException {
        try {
            handle.admin().deleteConsumerGroups(List.of(groupId)).all().get();
            return Map.of("group", groupId, "status", "deleted");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof GroupNotEmptyException) {
                return Map.of("group", groupId, "status", "delete-failed",
                        "reason", "Group is still active — stop its consumers first");
            }
            throw e;
        }
    }

    /**
     * Reset committed offsets for a group. Requires the group to be inactive (Empty state).
     * mode: EARLIEST | LATEST | OFFSET(value) | TIMESTAMP(epoch ms)
     */
    public Map<String, Object> resetOffsets(ClusterHandle handle, String groupId, String topic,
                                            String mode, Long value)
            throws ExecutionException, InterruptedException {
        var admin = handle.admin();

        ConsumerGroupDescription description = admin.describeConsumerGroups(List.of(groupId))
                .all().get().get(groupId);
        if (description != null && description.state() != ConsumerGroupState.DEAD
                && description.state() != ConsumerGroupState.EMPTY) {
            return Map.of("group", groupId, "status", "reset-failed",
                    "reason", "Group state is " + description.state() + " — offsets can only be reset while the group is inactive");
        }

        // Prefer partitions the group already committed to; fall back to all topic partitions.
        Set<TopicPartition> tps = new java.util.LinkedHashSet<>();
        try {
            admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get()
                    .keySet().stream().filter(tp -> tp.topic().equals(topic)).forEach(tps::add);
        } catch (ExecutionException e) {
            // fall through to full partition list
        }
        if (tps.isEmpty()) {
            admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).partitions()
                    .forEach(p -> tps.add(new TopicPartition(topic, p.partition())));
        }
        if (tps.isEmpty()) {
            throw new org.apache.kafka.common.errors.UnknownTopicOrPartitionException(topic);
        }

        Map<TopicPartition, OffsetAndMetadata> targets = new HashMap<>();
        switch (mode.toUpperCase()) {
            case "EARLIEST" -> admin.listOffsets(specs(tps, OffsetSpec.earliest())).all().get()
                    .forEach((tp, info) -> targets.put(tp, new OffsetAndMetadata(info.offset())));
            case "LATEST" -> admin.listOffsets(specs(tps, OffsetSpec.latest())).all().get()
                    .forEach((tp, info) -> targets.put(tp, new OffsetAndMetadata(info.offset())));
            case "OFFSET" -> tps.forEach(tp -> targets.put(tp, new OffsetAndMetadata(value)));
            case "TIMESTAMP" -> {
                Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> timeSpecs = new HashMap<>();
                tps.forEach(tp -> timeSpecs.put(tp, OffsetSpec.forTimestamp(value)));
                admin.listOffsets(timeSpecs).all().get().forEach((tp, info) -> {
                    if (info.offset() >= 0) {
                        targets.put(tp, new OffsetAndMetadata(info.offset()));
                    }
                });
                // partitions without a record past the timestamp fall back to latest
                Set<TopicPartition> missing = new java.util.LinkedHashSet<>(tps);
                missing.removeAll(targets.keySet());
                if (!missing.isEmpty()) {
                    admin.listOffsets(specs(missing, OffsetSpec.latest())).all().get()
                            .forEach((tp, info) -> targets.put(tp, new OffsetAndMetadata(info.offset())));
                }
            }
            default -> throw new IllegalArgumentException("Unknown reset mode '" + mode
                    + "' (use EARLIEST, LATEST, OFFSET or TIMESTAMP)");
        }

        admin.alterConsumerGroupOffsets(groupId, targets).all().get();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("group", groupId);
        result.put("topic", topic);
        result.put("mode", mode.toUpperCase());
        result.put("partitionsReset", targets.size());
        result.put("status", "reset");
        return result;
    }

    private static Map<TopicPartition, OffsetSpec> specs(java.util.Set<TopicPartition> tps, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> map = new HashMap<>();
        tps.forEach(tp -> map.put(tp, spec));
        return map;
    }

    public record GroupSummary(String groupId, String state, int committedPartitions, long totalLag) {
    }

    public record PartitionLag(int partition, long committedOffset, Long endOffset, long lag) {
    }

    public record MemberView(String consumerId, String clientId, String host, List<String> assignments) {
    }

    public record GroupDetail(String groupId, String state, Integer coordinatorId, List<MemberView> members,
                              Map<String, List<PartitionLag>> partitionsByTopic, long totalLag,
                              int committedPartitions) {
    }
}
