package com.ritikbansod.kafkawrapper.consumer;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class ConsumerController {

    private final DynamicConsumerService consumerService;
    private final KafkaClusterManager manager;

    public ConsumerController(DynamicConsumerService consumerService, KafkaClusterManager manager) {
        this.consumerService = consumerService;
        this.manager = manager;
    }

    public record SubscribeRequest(@NotBlank String groupId, @NotBlank String topic) {
    }

    @PostMapping("/api/clusters/{clusterId}/consumers")
    public Map<String, String> subscribe(@PathVariable String clusterId,
                                         @Valid @RequestBody SubscribeRequest request) {
        return consumerService.subscribe(clusterId, request.groupId(), request.topic());
    }

    @DeleteMapping("/api/clusters/{clusterId}/consumers/{groupId}/{topic}")
    public Map<String, String> unsubscribe(@PathVariable String clusterId, @PathVariable String groupId,
                                           @PathVariable String topic) {
        return consumerService.unsubscribe(clusterId, groupId, topic);
    }

    @GetMapping("/api/clusters/{clusterId}/consumers/{groupId}/messages")
    public List<DynamicConsumerService.ReceivedMessage> received(@PathVariable String clusterId,
                                                                 @PathVariable String groupId) {
        return consumerService.received(groupId);
    }

    @GetMapping("/api/clusters/{clusterId}/consumers")
    public Map<String, Boolean> running(@PathVariable String clusterId) {
        return consumerService.running();
    }

    // ---- legacy API (backward compatible, targets the default cluster) ----

    @PostMapping("/api/consumers")
    public Map<String, String> subscribeLegacy(@Valid @RequestBody SubscribeRequest request) {
        return consumerService.subscribe(KafkaClusterManager.DEFAULT_CLUSTER_ID,
                request.groupId(), request.topic());
    }

    @DeleteMapping("/api/consumers/{groupId}/{topic}")
    public Map<String, String> unsubscribeLegacy(@PathVariable String groupId, @PathVariable String topic) {
        return consumerService.unsubscribe(KafkaClusterManager.DEFAULT_CLUSTER_ID, groupId, topic);
    }

    @GetMapping("/api/consumers/{groupId}/messages")
    public List<DynamicConsumerService.ReceivedMessage> receivedLegacy(@PathVariable String groupId) {
        return consumerService.received(groupId);
    }

    @GetMapping("/api/consumers")
    public Map<String, Boolean> runningLegacy() {
        return consumerService.running();
    }
}
