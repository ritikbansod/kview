package com.ritikbansod.kafkawrapper.message;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/messages")
public class MessageController {

    private final ProducerService producerService;

    public MessageController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> produce(@RequestBody ProduceMessageRequest request) throws Exception {
        CompletableFuture<SendResult<String, String>> future =
                producerService.send(request.topic(), request.key(), request.payload());

        // Block briefly so the caller gets the assigned partition/offset back.
        SendResult<String, String> result = future.get(10, TimeUnit.SECONDS);

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", request.topic());
        body.put("key", request.key());
        body.put("partition", result.getRecordMetadata().partition());
        body.put("offset", result.getRecordMetadata().offset());
        return ResponseEntity.status(HttpStatus.CREATED).body(body);
    }
}
