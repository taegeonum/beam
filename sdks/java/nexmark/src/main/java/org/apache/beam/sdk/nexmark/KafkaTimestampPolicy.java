package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class KafkaTimestampPolicy extends TimestampPolicy implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaTimestampPolicy.class);
    private long timestamp = -1;

    @Override
    public Instant getTimestampForRecord(PartitionContext ctx, KafkaRecord record) {
        LOG.info("GetTimestamp: {}", this);

        if (record.getTimestamp() > timestamp) {
            LOG.info("Timestamp set: {}", record.getTimestamp());
            timestamp = record.getTimestamp();
        }
        return new Instant(record.getTimestamp());
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        LOG.info("GetWatermark {}", this);
        return new Instant(timestamp);
    }
}
