package com.ritikbansod.kafkawrapper.cluster;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.AuthorizationException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Cluster-level introspection: overview KPIs, broker list/configs, ACLs.
 */
@Service
public class ClusterService {

    private static final int TIMEOUT_MS = 10_000;

    public Map<String, Object> overview(ClusterHandle handle) throws ExecutionException, InterruptedException {
        Admin admin = handle.admin();
        Map<String, Object> overview = new LinkedHashMap<>();

        DescribeClusterResult describe = admin.describeCluster(new DescribeClusterOptions().timeoutMs(TIMEOUT_MS));
        Collection<Node> nodes = describe.nodes().get();
        Node controller = describe.controller().get();

        overview.put("clusterId", sanitizeClusterId(describe.clusterId().get()));
        overview.put("brokers", nodeViews(nodes));
        overview.put("controller", controller == null ? null : controller.id());

        Set<String> topicNames = admin.listTopics(new ListTopicsOptions().timeoutMs(TIMEOUT_MS).listInternal(true))
                .names().get();
        overview.put("topicCount", topicNames.size());

        DescribeTopicsResult described = admin.describeTopics(topicNames);

        int totalPartitions = 0;
        int underReplicated = 0;
        int offline = 0;
        Set<TopicPartition> dataPartitions = new java.util.HashSet<>();
        for (Map.Entry<String, TopicDescription> entry : described.allTopicNames().get().entrySet()) {
            String topic = entry.getKey();
            TopicDescription description = entry.getValue();
            totalPartitions += description.partitions().size();
            for (TopicPartitionInfo partition : description.partitions()) {
                if (partition.isr().size() < partition.replicas().size()) {
                    underReplicated++;
                }
                if (partition.leader() == null) {
                    offline++;
                }
            }
            if (!topic.startsWith("__")) {
                for (TopicPartitionInfo partition : description.partitions()) {
                    dataPartitions.add(new TopicPartition(topic, partition.partition()));
                }
            }
        }
        overview.put("partitionCount", totalPartitions);
        overview.put("underReplicatedPartitions", underReplicated);
        overview.put("offlinePartitions", offline);

        long estimatedMessages = 0;
        try {
            Map<TopicPartition, ListOffsetsResultInfo> ends = admin.listOffsets(specs(dataPartitions,
                    org.apache.kafka.clients.admin.OffsetSpec.latest())).all().get();
            Map<TopicPartition, ListOffsetsResultInfo> beginnings = admin.listOffsets(specs(dataPartitions,
                    org.apache.kafka.clients.admin.OffsetSpec.earliest())).all().get();
            for (TopicPartition tp : dataPartitions) {
                ListOffsetsResultInfo end = ends.get(tp);
                ListOffsetsResultInfo beginning = beginnings.get(tp);
                if (end != null && beginning != null) {
                    estimatedMessages += Math.max(0, end.offset() - beginning.offset());
                }
            }
        } catch (Exception e) {
            overview.put("estimatedMessagesError", rootMessage(e));
        }
        overview.put("estimatedMessages", estimatedMessages);

        try {
            overview.put("consumerGroupCount", admin.listConsumerGroups().valid().get().size());
        } catch (Exception e) {
            overview.put("consumerGroupCount", -1);
        }
        return overview;
    }

    public List<Map<String, Object>> brokers(ClusterHandle handle) throws ExecutionException, InterruptedException {
        Collection<Node> nodes = handle.admin()
                .describeCluster(new DescribeClusterOptions().timeoutMs(TIMEOUT_MS))
                .nodes().get();
        return nodeViews(nodes);
    }

    /**
     * Per-broker partition distribution: how many partitions each broker hosts
     * as leader vs follower, plus under-replicated counts and skew detection.
     */
    public Map<String, Object> brokerDistribution(ClusterHandle handle) throws ExecutionException, InterruptedException {
        var admin = handle.admin();
        DescribeClusterResult describe = admin.describeCluster(new DescribeClusterOptions().timeoutMs(TIMEOUT_MS));
        Collection<Node> nodes = describe.nodes().get();
        Set<String> topicNames = admin.listTopics(new ListTopicsOptions().timeoutMs(TIMEOUT_MS).listInternal(true))
                .names().get();
        Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames)
                .allTopicNames().get();

        // init per-broker counters
        Map<Integer, Map<String, Object>> brokerStats = new LinkedHashMap<>();
        for (Node node : nodes) {
            Map<String, Object> stats = new LinkedHashMap<>();
            stats.put("id", node.id());
            stats.put("host", node.host());
            stats.put("port", node.port());
            stats.put("leaderCount", 0);
            stats.put("followerCount", 0);
            stats.put("totalPartitions", 0);
            stats.put("underReplicated", 0);
            brokerStats.put(node.id(), stats);
        }

        int totalLeaders = 0;
        int totalReplicas = 0;
        for (TopicDescription td : descriptions.values()) {
            for (TopicPartitionInfo p : td.partitions()) {
                totalReplicas += p.replicas().size();
                if (p.leader() != null) {
                    brokerStats.computeIfAbsent(p.leader().id(), k -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("id", k);
                        m.put("leaderCount", 0);
                        m.put("followerCount", 0);
                        m.put("totalPartitions", 0);
                        m.put("underReplicated", 0);
                        return m;
                    });
                    int cur = (int) brokerStats.get(p.leader().id()).get("leaderCount");
                    brokerStats.get(p.leader().id()).put("leaderCount", cur + 1);
                }
                for (Node replica : p.replicas()) {
                    brokerStats.computeIfAbsent(replica.id(), k -> {
                        Map<String, Object> m = new LinkedHashMap<>();
                        m.put("id", k);
                        m.put("leaderCount", 0);
                        m.put("followerCount", 0);
                        m.put("totalPartitions", 0);
                        m.put("underReplicated", 0);
                        return m;
                    });
                    int cur = (int) brokerStats.get(replica.id()).get("totalPartitions");
                    brokerStats.get(replica.id()).put("totalPartitions", cur + 1);
                    if (p.leader() != null && replica.id() != p.leader().id()) {
                        int fc = (int) brokerStats.get(replica.id()).get("followerCount");
                        brokerStats.get(replica.id()).put("followerCount", fc + 1);
                    }
                    if (p.isr().size() < p.replicas().size()) {
                        int ur = (int) brokerStats.get(replica.id()).get("underReplicated");
                        brokerStats.get(replica.id()).put("underReplicated", ur + 1);
                    }
                }
            }
        }

        totalLeaders = (int) brokerStats.values().stream()
                .mapToInt(m -> (int) m.get("leaderCount")).sum();

        // skew detection: leader count per broker
        List<Integer> leaderCounts = brokerStats.values().stream()
                .map(m -> (int) m.get("leaderCount")).sorted().toList();
        int minLeaders = leaderCounts.isEmpty() ? 0 : leaderCounts.get(0);
        int maxLeaders = leaderCounts.isEmpty() ? 0 : leaderCounts.get(leaderCounts.size() - 1);
        boolean balanced = nodes.size() > 1 ? (maxLeaders - minLeaders) <= Math.max(1, totalLeaders / nodes.size() / 2) : true;

        List<Map<String, Object>> brokerList = brokerStats.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> {
                    Map<String, Object> m = new LinkedHashMap<>(e.getValue());
                    Node node = nodes.stream().filter(n -> n.id() == e.getKey()).findFirst().orElse(null);
                    m.put("host", node != null ? node.host() : "?");
                    m.put("port", node != null ? node.port() : 0);
                    return m;
                }).collect(java.util.stream.Collectors.toList());

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("brokers", brokerList);
        result.put("total", Map.of(
                "leaderCount", totalLeaders,
                "totalReplicas", totalReplicas,
                "brokerCount", nodes.size()));
        result.put("balanced", balanced);
        result.put("skewNote", balanced ? "" :
                "Leader distribution is uneven across brokers \u2014 consider running a preferred leader election or rebalancing");
        return result;
    }

    public List<Map<String, Object>> brokerConfigs(ClusterHandle handle, int brokerId)
            throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
        Config config = handle.admin().describeConfigs(List.of(resource)).all().get().get(resource);
        List<Map<String, Object>> entries = new ArrayList<>();
        for (ConfigEntry entry : config.entries()) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("name", entry.name());
            view.put("value", entry.isSensitive() ? "••••••" : entry.value());
            view.put("source", entry.source().name());
            view.put("sensitive", entry.isSensitive());
            view.put("readOnly", entry.isReadOnly());
            entries.add(view);
        }
        return entries;
    }

    public Map<String, Object> acls(ClusterHandle handle) {
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> bindings = new ArrayList<>();
        try {
            for (AclBinding binding : handle.admin().describeAcls(
                    org.apache.kafka.common.acl.AclBindingFilter.ANY).values().get()) {
                Map<String, Object> view = new LinkedHashMap<>();
                view.put("resourceType", binding.pattern().resourceType().name());
                view.put("resourceName", binding.pattern().name());
                view.put("patternType", binding.pattern().patternType().name());
                view.put("principal", binding.entry().principal());
                view.put("operation", binding.entry().operation().name());
                view.put("permissionType", binding.entry().permissionType().name());
                view.put("host", binding.entry().host());
                bindings.add(view);
            }
            result.put("supported", true);
        } catch (AuthorizationException | UnsupportedOperationException e) {
            result.put("supported", false);
            result.put("reason", "The connected principal is not authorized to read ACLs: " + rootMessage(e));
        } catch (Exception e) {
            result.put("supported", false);
            result.put("reason", rootMessage(e));
        }
        result.put("bindings", bindings);
        return result;
    }

    /**
     * Some broker versions (e.g. apache/kafka:3.7.0) report the cluster id wrapped
     * in Scala/Optional toString; unwrap so the UI shows the bare id.
     */
    public static String sanitizeClusterId(String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.trim();
        if (value.startsWith("Optional[")) {
            value = value.substring("Optional[".length(), value.endsWith("]") ? value.length() - 1 : value.length());
        } else if (value.startsWith("Some(") && value.endsWith(")")) {
            value = value.substring("Some(".length(), value.length() - 1);
        }
        return value.trim();
    }

    private static List<Map<String, Object>> nodeViews(Collection<Node> nodes) {
        return nodes.stream()
                .map(node -> {
                    Map<String, Object> view = new LinkedHashMap<>();
                    view.put("id", node.id());
                    view.put("host", node.host());
                    view.put("port", node.port());
                    view.put("rack", node.rack());
                    return view;
                })
                .sorted((a, b) -> Integer.compare((int) a.get("id"), (int) b.get("id")))
                .toList();
    }

    private static Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> specs(
            Set<TopicPartition> tps, org.apache.kafka.clients.admin.OffsetSpec spec) {
        Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> specs = new HashMap<>();
        for (TopicPartition tp : tps) {
            specs.put(tp, spec);
        }
        return specs;
    }

    private static String rootMessage(Throwable e) {
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
}
