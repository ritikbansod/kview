package com.ritikbansod.kafkawrapper.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ritikbansod.kafkawrapper.connection.ConnectionProfile;
import com.ritikbansod.kafkawrapper.connection.ConnectionStore;
import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka stores no partition-leader history — leaders move silently (leader
 * election, broker restarts, reassignment, ISR churn). This service samples
 * the partition state of every configured cluster on a schedule, diffs it
 * against the previous sample and records every change:
 *
 *   LEADER_CHANGED / LEADER_OFFLINE / ISR_CHANGED / REASSIGNED /
 *   PARTITION_ADDED / PARTITION_REMOVED
 *
 * History starts when the wrapper first observes the cluster — Kafka itself
 * keeps no such record. State and events persist across wrapper restarts.
 */
@Service
public class PartitionHistoryService {

    private static final Logger log = LoggerFactory.getLogger(PartitionHistoryService.class);
    private static final int MAX_EVENTS_PER_CLUSTER = 2_000;

    public record ChangeEvent(long ts, String topic, int partition, String type,
                              Integer fromLeader, Integer toLeader,
                              List<Integer> replicas, List<Integer> isr, String detail) { }

    public record ReplicaSnapshot(int leader, List<Integer> replicas, List<Integer> isr) { }

    public static final class ClusterHistory {
        public long firstSeen;
        public long lastPoll;
        public Map<String, ReplicaSnapshot> state = new HashMap<>();
        public List<ChangeEvent> events = new ArrayList<>();
    }

    private final KafkaClusterManager manager;
    private final ConnectionStore connectionStore;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Path file;
    private final Map<String, ClusterHistory> byCluster = new ConcurrentHashMap<>();

    public PartitionHistoryService(KafkaClusterManager manager, ConnectionStore connectionStore,
                                   @Value("${kview.data-dir:./data}") String dataDir) {
        this.manager = manager;
        this.connectionStore = connectionStore;
        this.file = Path.of(dataDir, "partition-history.json");
        load();
    }

    // ---------- scheduler ----------

    @Scheduled(fixedDelayString = "${kview.partition-history.interval-ms:15000}")
    public void sampleAll() {
        sampleCluster(KafkaClusterManager.DEFAULT_CLUSTER_ID);
        for (ConnectionProfile p : connectionStore.all()) {
            sampleCluster(p.id());
        }
    }

    public void sampleCluster(String clusterId) {
        ClusterHistory history = byCluster.computeIfAbsent(clusterId, id -> new ClusterHistory());
        long now = System.currentTimeMillis();
        Map<String, ReplicaSnapshot> current;
        try {
            var handle = manager.get(clusterId);
            var admin = handle.admin();
            var names = admin.listTopics(new org.apache.kafka.clients.admin.ListTopicsOptions()
                    .listInternal(true).timeoutMs(10_000)).names().get();
            DescribeTopicsResult described = admin.describeTopics(names,
                    new org.apache.kafka.clients.admin.DescribeTopicsOptions().timeoutMs(10_000));
            Map<String, TopicDescription> descriptions = described.allTopicNames().get();
            current = new HashMap<>();
            for (Map.Entry<String, TopicDescription> e : descriptions.entrySet()) {
                for (TopicPartitionInfo p : e.getValue().partitions()) {
                    String key = e.getKey() + "/" + p.partition();
                    current.put(key, new ReplicaSnapshot(
                            p.leader() == null ? -1 : p.leader().id(),
                            p.replicas().stream().map(node -> node.id()).sorted().toList(),
                            p.isr().stream().map(node -> node.id()).sorted().toList()));
                }
            }
        } catch (Exception e) {
            log.debug("Partition history sample skipped for '{}': {}", clusterId, e.getMessage());
            return;
        }

        synchronized (history) {
            if (history.firstSeen == 0) history.firstSeen = now;
            history.lastPoll = now;
            List<ChangeEvent> changes = diff(clusterId, history.state, current, now);
            if (!history.state.isEmpty() || !changes.isEmpty()) {
                history.events.addAll(changes);
                trim(history);
            }
            history.state = current;
            if (!changes.isEmpty()) {
                persist();
                changes.forEach(c -> log.info("[{}] partition history: {} {} p{} {}",
                        clusterId, c.type(), c.topic(), c.partition(), c.detail()));
            }
        }
    }

    // ---------- pure diff (unit-tested) ----------

    static List<ChangeEvent> diff(String clusterId, Map<String, ReplicaSnapshot> previous,
                                  Map<String, ReplicaSnapshot> current, long ts) {
        List<ChangeEvent> events = new ArrayList<>();
        for (Map.Entry<String, ReplicaSnapshot> e : current.entrySet()) {
            String key = e.getKey();
            ReplicaSnapshot now = e.getValue();
            ReplicaSnapshot before = previous.get(key);
            if (before == null) {
                events.add(new ChangeEvent(ts, topicOf(key), partitionOf(key), "PARTITION_ADDED",
                        null, now.leader(), now.replicas(), now.isr(),
                        "observed first time — leader on broker " + now.leader()));
                continue;
            }
            if (now.leader() != before.leader()) {
                if (now.leader() <= 0) {
                    events.add(new ChangeEvent(ts, topicOf(key), partitionOf(key), "LEADER_OFFLINE",
                            before.leader(), null, now.replicas(), now.isr(),
                            "leader moved off broker " + before.leader() + " — partition has no leader"));
                } else {
                    boolean preferred = !now.replicas().isEmpty() && now.replicas().get(0) == now.leader();
                    events.add(new ChangeEvent(ts, topicOf(key), partitionOf(key), "LEADER_CHANGED",
                            before.leader(), now.leader(), now.replicas(), now.isr(),
                            "leader moved broker " + before.leader() + " → " + now.leader()
                                    + (preferred ? " (preferred leader)" : "")));
                }
            } else if (!now.replicas().equals(before.replicas())) {
                events.add(new ChangeEvent(ts, topicOf(key), partitionOf(key), "REASSIGNED",
                        now.leader(), now.leader(), now.replicas(), now.isr(),
                        "replicas reassigned: " + before.replicas() + " → " + now.replicas()
                                + (now.isr().equals(now.replicas()) ? "" : " (ISR catching up: " + now.isr() + ")")));
            } else if (!now.isr().equals(before.isr())) {
                List<Integer> lost = new ArrayList<>(before.isr());
                lost.removeAll(now.isr());
                List<Integer> gained = new ArrayList<>(now.isr());
                gained.removeAll(before.isr());
                String detail = (lost.isEmpty() ? "" : "ISR shrunk (lost " + lost + ") ")
                        + (gained.isEmpty() ? "" : "ISR expanded (+" + gained + ")");
                events.add(new ChangeEvent(ts, topicOf(key), partitionOf(key), "ISR_CHANGED",
                        now.leader(), now.leader(), now.replicas(), now.isr(), detail.trim()));
            }
        }
        for (String key : previous.keySet()) {
            if (!current.containsKey(key)) {
                events.add(new ChangeEvent(ts, topicOf(key), partitionOf(key), "PARTITION_REMOVED",
                        previous.get(key).leader(), null, previous.get(key).replicas(),
                        previous.get(key).isr(), "partition no longer present"));
            }
        }
        return events;
    }

    private static String topicOf(String key) {
        int slash = key.lastIndexOf('/');
        return slash < 0 ? key : key.substring(0, slash);
    }

    private static int partitionOf(String key) {
        int slash = key.lastIndexOf('/');
        return slash < 0 ? -1 : Integer.parseInt(key.substring(slash + 1));
    }

    private static void trim(ClusterHistory history) {
        if (history.events.size() > MAX_EVENTS) {
            history.events = new ArrayList<>(
                    history.events.subList(history.events.size() - MAX_EVENTS_PER_CLUSTER, history.events.size()));
        }
    }

    private static final int MAX_EVENTS = MAX_EVENTS_PER_CLUSTER;

    // ---------- queries ----------

    public Map<String, Object> historyFor(String clusterId, String topicFilter, int limit, int offset) {
        ClusterHistory history = byCluster.get(clusterId);
        Map<String, Object> result = new LinkedHashMap<>();
        if (history == null) {
            result.put("monitored", false);
            result.put("events", List.of());
            result.put("total", 0);
            result.put("hasMore", false);
            return result;
        }
        synchronized (history) {
            List<ChangeEvent> filtered = history.events.stream()
                    .filter(e -> topicFilter == null || topicFilter.isBlank() || e.topic().equals(topicFilter))
                    .toList();
            List<ChangeEvent> newestFirst = new ArrayList<>(filtered);
            java.util.Collections.reverse(newestFirst);
            int total = newestFirst.size();
            int from = Math.min(Math.max(offset, 0), total);
            int to = Math.min(from + Math.max(limit, 1), total);
            result.put("monitored", true);
            result.put("firstSeen", history.firstSeen);
            result.put("lastPoll", history.lastPoll);
            result.put("total", total);
            result.put("offset", from);
            result.put("hasMore", to < total);
            result.put("events", newestFirst.subList(from, to));
        }
        return result;
    }

    public synchronized void clear(String clusterId) {
        byCluster.remove(clusterId);
        persist();
    }

    // ---------- persistence ----------

    private void persist() {
        try {
            Files.createDirectories(file.getParent());
            Path temp = file.resolveSibling(file.getFileName() + ".tmp");
            mapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), byCluster);
            try {
                Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(temp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            log.error("Failed to persist partition history: {}", e.getMessage());
        }
    }

    private void load() {
        if (!Files.exists(file)) return;
        try {
            Map<String, ClusterHistory> parsed = mapper.readValue(file.toFile(),
                    mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, ClusterHistory.class));
            byCluster.putAll(parsed);
            log.info("Loaded partition history for {} cluster(s) from {}", byCluster.size(), file);
        } catch (IOException e) {
            log.error("Partition history file {} unreadable — starting empty", file, e);
        }
    }
}
