package com.ritikbansod.kafkawrapper.cluster;

import com.ritikbansod.kafkawrapper.connection.ClusterHandle;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Per-broker partition drill-down: which partitions a broker leads,
 * which it follows, and their ISR status.
 */
@Service
public class BrokerPartitionService {

    private static final int TIMEOUT_MS = 10_000;

    public Map<String, Object> partitionsOnBroker(ClusterHandle handle, int brokerId)
            throws ExecutionException, InterruptedException {
        var admin = handle.admin();
        Collection<Node> nodes = admin.describeCluster(new DescribeClusterOptions().timeoutMs(TIMEOUT_MS)).nodes().get();
        boolean brokerExists = nodes.stream().anyMatch(n -> n.id() == brokerId);
        if (!brokerExists) {
            throw new IllegalArgumentException("Broker " + brokerId + " does not exist in this cluster");
        }

            var topicNames = admin.listTopics(new ListTopicsOptions().listInternal(true).timeoutMs(TIMEOUT_MS))
                .names().get();
        DescribeTopicsResult described = admin.describeTopics(topicNames,
                new org.apache.kafka.clients.admin.DescribeTopicsOptions().timeoutMs(TIMEOUT_MS));
        Map<String, TopicDescription> descriptions = described.allTopicNames().get();

        List<Map<String, Object>> leader = new ArrayList<>();
        List<Map<String, Object>> follower = new ArrayList<>();
        int underReplicated = 0;

        for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
            String topic = entry.getKey();
            for (TopicPartitionInfo p : entry.getValue().partitions()) {
                boolean isLeader = p.leader() != null && p.leader().id() == brokerId;
                boolean isReplica = p.replicas().stream().anyMatch(r -> r.id() == brokerId);
                if (!isLeader && !isReplica) continue;

                boolean inSync = p.isr().stream().anyMatch(r -> r.id() == brokerId);
                Map<String, Object> info = new LinkedHashMap<>();
                info.put("topic", topic);
                info.put("partition", p.partition());
                info.put("leader", p.leader() == null ? -1 : p.leader().id());
                info.put("isr", p.isr().stream().map(Node::id).sorted().toList());
                info.put("replicas", p.replicas().stream().map(Node::id).sorted().toList());
                info.put("inSync", inSync);

                if (isLeader) {
                    leader.add(info);
                } else {
                    follower.add(info);
                }
                if (!inSync) underReplicated++;
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("brokerId", brokerId);
        result.put("leader", leader);
        result.put("follower", follower);
        result.put("underReplicatedCount", underReplicated);
        return result;
    }
}
