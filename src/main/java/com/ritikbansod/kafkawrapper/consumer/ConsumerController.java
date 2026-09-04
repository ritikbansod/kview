package com.ritikbansod.kafkawrapper.consumer;

import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consumers")
public class ConsumerController {

    private final DynamicConsumerService consumerService;

    public ConsumerController(DynamicConsumerService consumerService) {
        this.consumerService = consumerService;
    }

    public record SubscribeRequest(@NotBlank String groupId, @NotBlank String topic) {
    }

    @PostMapping
    public Map<String, String> subscribe(@RequestBody SubscribeRequest request) {
        return consumerService.subscribe(request.groupId(), request.topic());
    }

    @DeleteMapping("/{groupId}/{topic}")
    public Map<String, String> unsubscribe(@PathVariable String groupId, @PathVariable String topic) {
        return consumerService.unsubscribe(groupId, topic);
    }

    @GetMapping("/{groupId}/messages")
    public List<DynamicConsumerService.ReceivedMessage> received(@PathVariable String groupId) {
        return consumerService.received(groupId);
    }

    @GetMapping
    public Map<String, Boolean> running() {
        return consumerService.running();
    }
}
