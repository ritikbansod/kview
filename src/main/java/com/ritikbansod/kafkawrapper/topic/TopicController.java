package com.ritikbansod.kafkawrapper.topic;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@RestController
public class TopicController {

    private final TopicManagementService topicService;
    private final KafkaClusterManager manager;

    public TopicController(TopicManagementService topicService, KafkaClusterManager manager) {
        this.topicService = topicService;
        this.manager = manager;
    }

    public record CreateTopicRequest(@NotBlank String name, @Min(1) int partitions,
                                     @Min(1) short replicationFactor, Map<String, String> configs) {
    }

    public record IncreasePartitionsRequest(@Min(1) int totalPartitions) {
    }

    public record AlterConfigsRequest(@NotEmpty Map<String, String> configs) {
    }

    // ---- cluster-scoped API ----

    @GetMapping("/api/clusters/{clusterId}/topics")
    public List<TopicManagementService.TopicSummary> list(@PathVariable String clusterId,
                                                          @RequestParam(defaultValue = "true") boolean includeCounts)
            throws ExecutionException, InterruptedException {
        return topicService.list(manager.get(clusterId), includeCounts);
    }

    @GetMapping("/api/clusters/{clusterId}/topics/{topic}")
    public TopicManagementService.TopicDetail detail(@PathVariable String clusterId, @PathVariable String topic)
            throws ExecutionException, InterruptedException {
        return topicService.detail(manager.get(clusterId), topic);
    }

    @PostMapping("/api/clusters/{clusterId}/topics")
    public ResponseEntity<Map<String, Object>> create(@PathVariable String clusterId,
                                                      @Valid @RequestBody CreateTopicRequest request)
            throws ExecutionException, InterruptedException {
        Map<String, Object> result = topicService.create(manager.get(clusterId),
                request.name(), request.partitions(), request.replicationFactor(), request.configs());
        HttpStatus status = "created".equals(result.get("status")) ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(result);
    }

    @PostMapping("/api/clusters/{clusterId}/topics/{topic}/partitions")
    public Map<String, Object> increasePartitions(@PathVariable String clusterId, @PathVariable String topic,
                                                  @Valid @RequestBody IncreasePartitionsRequest request)
            throws ExecutionException, InterruptedException {
        return topicService.increasePartitions(manager.get(clusterId), topic, request.totalPartitions());
    }

    @PutMapping("/api/clusters/{clusterId}/topics/{topic}/configs")
    public Map<String, Object> alterConfigs(@PathVariable String clusterId, @PathVariable String topic,
                                            @Valid @RequestBody AlterConfigsRequest request)
            throws ExecutionException, InterruptedException {
        return topicService.alterConfigs(manager.get(clusterId), topic, request.configs());
    }

    @DeleteMapping("/api/clusters/{clusterId}/topics/{topic}")
    public Map<String, Object> delete(@PathVariable String clusterId, @PathVariable String topic)
            throws ExecutionException, InterruptedException {
        return topicService.delete(manager.get(clusterId), topic);
    }

    // ---- legacy API (backward compatible, targets the default cluster) ----

    @PostMapping("/api/topics")
    public ResponseEntity<Map<String, Object>> createLegacy(@Valid @RequestBody CreateTopicRequest request)
            throws ExecutionException, InterruptedException {
        return create(KafkaClusterManager.DEFAULT_CLUSTER_ID, request);
    }

    @GetMapping("/api/topics")
    public Set<String> listLegacy() throws ExecutionException, InterruptedException {
        return topicService.list(manager.get(KafkaClusterManager.DEFAULT_CLUSTER_ID), false)
                .stream().map(TopicManagementService.TopicSummary::name)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @GetMapping("/api/topics/{name}")
    public TopicManagementService.TopicDetail describeLegacy(@PathVariable String name)
            throws ExecutionException, InterruptedException {
        return detail(KafkaClusterManager.DEFAULT_CLUSTER_ID, name);
    }

    @DeleteMapping("/api/topics/{name}")
    public Map<String, Object> deleteLegacy(@PathVariable String name) throws ExecutionException, InterruptedException {
        return delete(KafkaClusterManager.DEFAULT_CLUSTER_ID, name);
    }
}
