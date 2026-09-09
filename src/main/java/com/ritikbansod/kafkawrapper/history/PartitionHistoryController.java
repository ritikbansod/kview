package com.ritikbansod.kafkawrapper.history;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Partition leader/replica history recorded by the background sampler.
 */
@RestController
@RequestMapping("/api/clusters/{clusterId}")
public class PartitionHistoryController {

    private final PartitionHistoryService historyService;

    public PartitionHistoryController(PartitionHistoryService historyService) {
        this.historyService = historyService;
    }

    /** Change history for one topic (newest first, offset-paginated). */
    @GetMapping("/topics/{topic}/history")
    public Map<String, Object> topicHistory(@PathVariable String clusterId, @PathVariable String topic,
                                            @RequestParam(defaultValue = "200") int limit,
                                            @RequestParam(defaultValue = "0") int offset) {
        return historyService.historyFor(clusterId, topic, limit, offset);
    }

    /** Change history across all topics of the cluster (newest first, offset-paginated). */
    @GetMapping("/history")
    public Map<String, Object> clusterHistory(@PathVariable String clusterId,
                                              @RequestParam(defaultValue = "500") int limit,
                                              @RequestParam(defaultValue = "0") int offset) {
        return historyService.historyFor(clusterId, null, limit, offset);
    }

    /** Clears recorded history for the cluster (monitoring continues). */
    @DeleteMapping("/history")
    public Map<String, Object> clear(@PathVariable String clusterId) {
        historyService.clear(clusterId);
        return Map.of("clusterId", clusterId, "status", "cleared");
    }
}
