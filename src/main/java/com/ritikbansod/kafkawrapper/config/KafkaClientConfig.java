package com.ritikbansod.kafkawrapper.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes the raw Kafka AdminClient alongside Spring's KafkaAdmin,
 * so topic listing/deletion/description use the standard Admin API.
 */
@Configuration
public class KafkaClientConfig {

    @Bean
    public Admin kafkaAdminClient(KafkaProperties properties) {
        return AdminClient.create(properties.buildAdminProperties());
    }
}
