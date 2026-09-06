package com.ritikbansod.kafkawrapper.group;

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
import java.util.concurrent.ExecutionException;

@RestController
public class ConsumerGroupController {

    private final ConsumerGroupService groupService;
    private final KafkaClusterManager manager;

    public ConsumerGroupController(ConsumerGroupService groupService, KafkaClusterManager manager) {
        this.groupService = groupService;
        this.manager = manager;
    }

    public record ResetOffsetsRequest(@NotBlank String topic, @NotBlank String mode, Long value) {
    }

    @GetMapping("/api/clusters/{clusterId}/groups")
    public List<ConsumerGroupService.GroupSummary> list(@PathVariable String clusterId)
            throws ExecutionException, InterruptedException {
        return groupService.list(manager.get(clusterId));
    }

    @GetMapping("/api/clusters/{clusterId}/groups/{groupId}")
    public ConsumerGroupService.GroupDetail detail(@PathVariable String clusterId, @PathVariable String groupId)
            throws ExecutionException, InterruptedException {
        return groupService.detail(manager.get(clusterId), groupId);
    }

    @DeleteMapping("/api/clusters/{clusterId}/groups/{groupId}")
    public Map<String, Object> delete(@PathVariable String clusterId, @PathVariable String groupId)
            throws ExecutionException, InterruptedException {
        return groupService.delete(manager.get(clusterId), groupId);
    }

    @PostMapping("/api/clusters/{clusterId}/groups/{groupId}/offsets/reset")
    public Map<String, Object> resetOffsets(@PathVariable String clusterId, @PathVariable String groupId,
                                            @Valid @RequestBody ResetOffsetsRequest request)
            throws ExecutionException, InterruptedException {
        return groupService.resetOffsets(manager.get(clusterId), groupId,
                request.topic(), request.mode(), request.value());
    }
}
