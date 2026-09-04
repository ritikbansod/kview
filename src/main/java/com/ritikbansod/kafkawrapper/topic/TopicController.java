package com.ritikbansod.kafkawrapper.topic;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/topics")
public class TopicController {

    private final TopicManagementService topicService;

    public TopicController(TopicManagementService topicService) {
        this.topicService = topicService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@Valid @RequestBody CreateTopicRequest request) throws Exception {
        Map<String, Object> result = topicService.create(
                request.name(), request.partitions(), request.replicationFactor());
        HttpStatus status = "created".equals(result.get("status"))
                ? HttpStatus.CREATED
                : HttpStatus.OK;
        return ResponseEntity.status(status).body(result);
    }

    @GetMapping
    public Set<String> list() throws Exception {
        return topicService.list();
    }

    @GetMapping("/{name}")
    public Map<String, String> describe(@PathVariable String name) throws Exception {
        return topicService.describe(name);
    }

    @DeleteMapping("/{name}")
    public Map<String, String> delete(@PathVariable String name) throws Exception {
        return topicService.delete(name);
    }
}
