package com.ritikbansod.kafkawrapper.live;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/clusters/{clusterId}/topics/{topic}/tail")
public class LiveTailController {

    private final LiveTailService tailService;
    private final KafkaClusterManager manager;

    public LiveTailController(LiveTailService tailService, KafkaClusterManager manager) {
        this.tailService = tailService;
        this.manager = manager;
    }

    /**
     * Tail a topic as Server-Sent Events (browser EventSource is GET-only). Query params:
     * from=latest|earliest, partition=n, groupId=... , autoCommit=true|false
     */
    @GetMapping
    public SseEmitter tail(@PathVariable String clusterId, @PathVariable String topic,
                           @RequestParam(defaultValue = "latest") String from,
                           @RequestParam(required = false) Integer partition,
                           @RequestParam(required = false) String groupId,
                           @RequestParam(required = false) Boolean autoCommit) {
        LiveTailService.TailSession session = tailService.start(manager.get(clusterId), topic,
                new LiveTailService.TailRequest(from, partition, groupId, autoCommit));
        return session.emitter();
    }

    @GetMapping("/sessions")
    public List<LiveTailService.TailInfo> active() {
        return tailService.active();
    }

    @DeleteMapping("/{tailId}")
    public Map<String, Object> stop(@PathVariable String tailId) {
        boolean stopped = tailService.stop(tailId);
        return Map.of("tailId", tailId, "status", stopped ? "stopped" : "not-running");
    }
}
