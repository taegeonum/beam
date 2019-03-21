package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Optional;


public class KafkaTimestampPolicyFactory implements Serializable, TimestampPolicyFactory<Long, Event> {
    @Override
    public TimestampPolicy<Long, Event> createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        return new KafkaTimestampPolicy();
    }
}
