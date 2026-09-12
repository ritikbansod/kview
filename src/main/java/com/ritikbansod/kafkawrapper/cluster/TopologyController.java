package com.ritikbansod.kafkawrapper.cluster;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/clusters/{clusterId}")
public class TopologyController {

    private final KafkaClusterManager manager;
    private final ClusterTopologyService topologyService;

    public TopologyController(KafkaClusterManager manager, ClusterTopologyService topologyService) {
        this.manager = manager;
        this.topologyService = topologyService;
    }

    @GetMapping("/topology")
    public ClusterTopologyService.Topology topology(@PathVariable String clusterId)
            throws ExecutionException, InterruptedException {
        return topologyService.topology(manager.get(clusterId));
    }
}
