package com.ritikbansod.kafkawrapper.topic;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

/**
 * Request body for topic creation.
 */
public record CreateTopicRequest(
        @NotBlank String name,
        @Min(1) int partitions,
        @Min(1) short replicationFactor) {
}
