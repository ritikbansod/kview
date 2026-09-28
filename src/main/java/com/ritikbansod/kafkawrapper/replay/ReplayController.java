package com.ritikbansod.kafkawrapper.replay;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Bulk replay endpoint — a real mutation (produces to the target topic), so the
 * safety chain gates it: admin role, audit log, refused in read-only mode.
 */
@RestController
@RequestMapping("/api/clusters/{clusterId}")
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    @PostMapping("/topics/{topic}/replay")
    public ReplayService.ReplayResult replay(@PathVariable String clusterId, @PathVariable String topic,
                                             @RequestBody ReplayService.ReplayRequest request) {
        return replayService.replay(clusterId, topic, request);
    }
}
