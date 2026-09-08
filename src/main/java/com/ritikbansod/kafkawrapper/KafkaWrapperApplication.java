package com.ritikbansod.kafkawrapper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaWrapperApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaWrapperApplication.class, args);
    }
}
