package org.apache.beam.sdk.nexmark;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TimestampedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stamp {
    private static final Logger LOG = LoggerFactory.getLogger(Stamp.class);
    /** Return a transform to make explicit the timestamp of each element. */
    public static <T> ParDo.SingleOutput<T, TimestampedValue<T>> stamp(String name) {
        return ParDo.of(
                new DoFn<T, TimestampedValue<T>>() {

                    final long windowSize = 2000;
                    long prevWindowTime = System.currentTimeMillis();
                    long totalLatency = 0;
                    int numEvents = 0;

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        final long currTime = System.currentTimeMillis();
                        totalLatency += (currTime - c.timestamp().getMillis());
                        numEvents += 1;

                        if (currTime - prevWindowTime > windowSize) {
                            LOG.info("Avg Latency (in {} window): {} (# events: {})",
                                    (currTime - prevWindowTime),
                                    (totalLatency / numEvents), numEvents);

                            totalLatency = 0;
                            numEvents = 0;
                            prevWindowTime = currTime;
                        }

                        c.output(TimestampedValue.of(c.element(), c.timestamp()));
                    }
                });
    }
}
