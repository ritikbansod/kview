package com.ritikbansod.kafkawrapper.message;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Produces messages through the shared KafkaTemplate.
 */
@Service
public class ProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, String>> send(String topic, String key, String payload) {
        return kafkaTemplate.send(topic, key, payload);
    }
}
