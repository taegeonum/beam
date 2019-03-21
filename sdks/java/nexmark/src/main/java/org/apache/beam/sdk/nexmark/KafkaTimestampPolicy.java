package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.joda.time.Instant;

import java.io.Serializable;


public class KafkaTimestampPolicy extends TimestampPolicy implements Serializable {

    private long timestamp = -1;

    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord record) {
        if (record.getTimestamp() > timestamp) {
            timestamp = record.getTimestamp();
        }
        return new Instant(record.getTimestamp());
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        return new Instant(timestamp);
    }
}
