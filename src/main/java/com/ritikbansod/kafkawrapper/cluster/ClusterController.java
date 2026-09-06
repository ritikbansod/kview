package com.ritikbansod.kafkawrapper.cluster;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/clusters/{clusterId}")
public class ClusterController {

    private final KafkaClusterManager manager;
    private final ClusterService clusterService;

    public ClusterController(KafkaClusterManager manager, ClusterService clusterService) {
        this.manager = manager;
        this.clusterService = clusterService;
    }

    @GetMapping("/overview")
    public Map<String, Object> overview(@PathVariable String clusterId)
            throws ExecutionException, InterruptedException {
        return clusterService.overview(manager.get(clusterId));
    }

    @GetMapping("/brokers")
    public List<Map<String, Object>> brokers(@PathVariable String clusterId)
            throws ExecutionException, InterruptedException {
        return clusterService.brokers(manager.get(clusterId));
    }

    @GetMapping("/brokers/{brokerId}/configs")
    public List<Map<String, Object>> brokerConfigs(@PathVariable String clusterId, @PathVariable int brokerId)
            throws ExecutionException, InterruptedException {
        if (manager.get(clusterId).admin().describeCluster(
                new org.apache.kafka.clients.admin.DescribeClusterOptions().timeoutMs(10_000))
                .nodes().get().stream().noneMatch(n -> n.id() == brokerId)) {
            throw new NoSuchElementException("Unknown broker id " + brokerId);
        }
        return clusterService.brokerConfigs(manager.get(clusterId), brokerId);
    }

    @GetMapping("/acls")
    public Map<String, Object> acls(@PathVariable String clusterId) {
        return clusterService.acls(manager.get(clusterId));
    }
}
