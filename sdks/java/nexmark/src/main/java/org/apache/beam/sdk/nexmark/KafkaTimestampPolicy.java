package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.joda.time.Instant;

import java.io.Serializable;


public class KafkaTimestampPolicy extends TimestampPolicy implements Serializable {

    private transient KafkaRecord prevRecord = null;

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
