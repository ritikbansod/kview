package com.ritikbansod.kafkawrapper.browse;

import com.ritikbansod.kafkawrapper.connection.KafkaClusterManager;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
public class MessageBrowserController {

    private final MessageBrowserService browserService;
    private final KafkaClusterManager manager;

    public MessageBrowserController(MessageBrowserService browserService, KafkaClusterManager manager) {
        this.browserService = browserService;
        this.manager = manager;
    }

    @PostMapping("/api/clusters/{clusterId}/topics/{topic}/browse")
    public MessageBrowserService.BrowseResult browse(@PathVariable String clusterId,
                                                     @PathVariable String topic,
                                                     @RequestBody MessageBrowserService.BrowseRequest request)
            throws ExecutionException, InterruptedException {
        return browserService.browse(manager.get(clusterId), topic, request);
    }
}
