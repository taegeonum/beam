package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Optional;


public class KafkaTimestampPolicyFactory implements Serializable, TimestampPolicyFactory<Long, Event> {
    @Override
    public TimestampPolicy<Long, Event> createTimestampPolicy(TopicPartition tp, Optional<Instant> previousWatermark) {
        //return new CustomTimestampPolicyWithLimitedDelay(new TimestampFunc(), Duration.standardSeconds(3), Optional.empty());
        return new KafkaTimestampPolicy();
    }

    public static final class TimestampFunc implements SerializableFunction<KafkaRecord<Long, Event>, Instant> {

        @Override
        public Instant apply(KafkaRecord<Long, Event> input) {
            return new Instant(input.getTimestamp());
        }
    }
}
