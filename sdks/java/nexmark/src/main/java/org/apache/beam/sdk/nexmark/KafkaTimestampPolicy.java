package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

import static com.google.common.base.Preconditions.checkArgument;

public class KafkaTimestampPolicy extends TimestampPolicy {

    private KafkaRecord prevRecord = null;

    SerializableFunction<KafkaRecord<Long, Event>, Instant> timestampFunction =
            record -> {
                checkArgument(
                        record.getTimestampType() == KafkaTimestampType.CREATE_TIME,
                        "Kafka record's timestamp is not 'CREATE_TIME' "
                                + "(topic: %s, partition %s, offset %s, timestamp type '%s')",
                        record.getTopic(),
                        record.getPartition(),
                        record.getOffset(),
                        record.getTimestampType());
                return new Instant(record.getTimestamp());
            };

    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord record) {
        prevRecord = record;
        return new Instant(record.getTimestamp());
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        if (prevRecord == null) {
            return new Instant(-1);
        } else {
            return new Instant(prevRecord.getTimestamp());
        }
    }
}
