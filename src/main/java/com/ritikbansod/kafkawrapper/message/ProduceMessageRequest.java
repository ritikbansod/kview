package com.ritikbansod.kafkawrapper.message;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Request body for producing a message.
 */
public record ProduceMessageRequest(
        @NotBlank String topic,
        @Size(max = 256) String key,
        @NotBlank String payload) {
}
