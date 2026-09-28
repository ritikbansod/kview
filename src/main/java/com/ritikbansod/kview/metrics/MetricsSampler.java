package com.ritikbansod.kview.metrics;

import com.ritikbansod.kview.cluster.ClusterService;
import com.ritikbansod.kview.connection.ClusterHandle;
import com.ritikbansod.kview.connection.ConnectionStore;
import com.ritikbansod.kview.connection.KafkaClusterManager;
import com.ritikbansod.kview.group.ConsumerGroupService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Samples per-cluster health and consumer-group lag on a fixed schedule and
 * keeps the latest snapshot in memory, so a Prometheus scrape never blocks on
 * broker calls. Micrometer gauges read the sampled values; series appear and
 * disappear with the clusters and consumer groups they describe.
 *
 * Exposed series: kview_cluster_topics, kview_cluster_partitions,
 * kview_cluster_under_replicated_partitions, kview_cluster_offline_partitions,
 * kview_cluster_consumer_groups, kview_cluster_reachable (all with a
 * {@code cluster} tag) and kview_consumer_group_lag (tags {@code cluster},
 * {@code group}).
 */
@Service
public class MetricsSampler {

    private static final Logger log = LoggerFactory.getLogger(MetricsSampler.class);

    record ClusterSnapshot(boolean reachable, long topics, long partitions, long underReplicated,
                           long offline, long groups, Map<String, Long> groupLag) { }

    /** One registered gauge: the AtomicLong the gauge reads and its meter, for removal. */
    private record Series(AtomicLong value, Meter meter) { }

    private final KafkaClusterManager manager;
    private final ConnectionStore store;
    private final ClusterService clusterService;
    private final ConsumerGroupService groupService;
    private final MeterRegistry registry;
    private final Map<String, Series> series = new ConcurrentHashMap<>();
    private final Map<String, ClusterSnapshot> snapshots = new ConcurrentHashMap<>();

    public MetricsSampler(KafkaClusterManager manager, ConnectionStore store,
                          ClusterService clusterService, ConsumerGroupService groupService,
                          MeterRegistry registry) {
        this.manager = manager;
        this.store = store;
        this.clusterService = clusterService;
        this.groupService = groupService;
        this.registry = registry;
    }

    ClusterSnapshot snapshot(String clusterId) {
        ClusterSnapshot snapshot = snapshots.get(clusterId);
        return snapshot == null
                ? new ClusterSnapshot(false, 0, 0, 0, 0, 0, Map.of())
                : snapshot;
    }

    @Scheduled(fixedDelayString = "${kview.metrics.interval-ms:15000}")
    public void sample() {
        Set<String> clusterIds = new LinkedHashSet<>();
        clusterIds.add(KafkaClusterManager.DEFAULT_CLUSTER_ID);
        store.all().forEach(profile -> clusterIds.add(profile.id()));

        Set<String> liveSeries = new HashSet<>();
        for (String clusterId : clusterIds) {
            try {
                ClusterHandle handle = manager.get(clusterId);
                Map<String, Object> overview = clusterService.overview(handle);
                List<ConsumerGroupService.GroupSummary> groups = groupService.list(handle);
                Map<String, Long> lags = new LinkedHashMap<>();
                groups.forEach(group -> lags.put(group.groupId(), Math.max(0, group.totalLag())));
                snapshots.put(clusterId, new ClusterSnapshot(true,
                        num(overview.get("topicCount")), num(overview.get("partitionCount")),
                        num(overview.get("underReplicatedPartitions")), num(overview.get("offlinePartitions")),
                        groups.size(), lags));

                gauge("kview_cluster_topics", "Topics in the cluster", clusterId, null,
                        num(overview.get("topicCount")), liveSeries);
                gauge("kview_cluster_partitions", "Partitions in the cluster", clusterId, null,
                        num(overview.get("partitionCount")), liveSeries);
                gauge("kview_cluster_under_replicated_partitions", "Under-replicated partitions", clusterId, null,
                        num(overview.get("underReplicatedPartitions")), liveSeries);
                gauge("kview_cluster_offline_partitions", "Offline partitions", clusterId, null,
                        num(overview.get("offlinePartitions")), liveSeries);
                gauge("kview_cluster_consumer_groups", "Consumer groups in the cluster", clusterId, null,
                        groups.size(), liveSeries);
                gauge("kview_cluster_reachable", "1 when the last sample reached the cluster", clusterId, null,
                        1, liveSeries);
                lags.forEach((group, lag) ->
                        gauge("kview_consumer_group_lag", "Total lag of a consumer group", clusterId, group, lag, liveSeries));
            } catch (Exception e) {
                snapshots.put(clusterId, new ClusterSnapshot(false, 0, 0, 0, 0, 0, Map.of()));
                gauge("kview_cluster_reachable", "1 when the last sample reached the cluster", clusterId, null,
                        0, liveSeries);
                log.debug("Metrics sample skipped for cluster '{}': {}", clusterId, e.getMessage());
            }
        }

        series.entrySet().removeIf(entry -> {
            if (liveSeries.contains(entry.getKey())) return false;
            registry.remove(entry.getValue().meter().getId());
            return true;
        });
    }

    private void gauge(String name, String description, String clusterId, String group,
                       long value, Set<String> liveSeries) {
        String key = name + "|" + clusterId + "|" + (group == null ? "" : group);
        liveSeries.add(key);
        Series existing = series.get(key);
        if (existing != null) {
            existing.value().set(value);
            return;
        }
        AtomicLong holder = new AtomicLong(value);
        Gauge.Builder builder = Gauge.builder(name, holder, AtomicLong::get)
                .description(description)
                .tag("cluster", clusterId);
        if (group != null) {
            builder.tag("group", group);
        }
        series.put(key, new Series(holder, builder.register(registry)));
    }

    private static long num(Object value) {
        return value instanceof Number number ? number.longValue() : 0;
    }
}
