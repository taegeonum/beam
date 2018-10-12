/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.nexmark.queries;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.nexmark.NexmarkConfiguration;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Query 5, 'Hot Items'. Which auctions have seen the most bids in the last hour (updated every
 * minute). In CQL syntax:
 *
 * <pre>{@code
 * SELECT Rstream(auction)
 * FROM (SELECT B1.auction, count(*) AS num
 *       FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B1
 *       GROUP BY B1.auction)
 * WHERE num >= ALL (SELECT count(*)
 *                   FROM Bid [RANGE 60 MINUTE SLIDE 1 MINUTE] B2
 *                   GROUP BY B2.auction);
 * }</pre>
 *
 * <p>To make things a bit more dynamic and easier to test we use much shorter windows, and we'll
 * also preserve the bid counts.
 */
public class Query5Fix extends NexmarkQuery {
  public Query5Fix(NexmarkConfiguration configuration) {
    super(configuration, "Query5Fix");
  }

  private PCollection<TimestampedValue<KnownSize>> applyTyped(PCollection<Event> events) {
    return events
        // Only want the bid events.
        .apply(JUST_BIDS)
        // Window the bids into sliding windows.
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(configuration.windowSizeSec))
                    .every(Duration.standardSeconds(configuration.windowPeriodSec))))
        // Project just the auction id.
        .apply("BidToAuction", BID_TO_AUCTION)

        // Count the number of bids per auction id.
        .apply(new MyPerElement<>())
        .apply(
                name + ".ToServerless",
                ParDo.of(new ToServerless()))
            .setCoder(makeCastingCoder((Coder) StringUtf8Coder.of()));
  }


    /** Return a coder for {@code KnownSize} that are known to be exactly of type {@code T}. */
    private static <T extends KnownSize> Coder<KnownSize> makeCastingCoder(Coder<T> trueCoder) {
        return new CastingCoder<>(trueCoder);
    }


    /**
     * A coder for instances of {@code T} cast up to {@link KnownSize}.
     *
     * @param <T> True type of object.
     */
    private static class CastingCoder<T extends KnownSize> extends CustomCoder<KnownSize> {
        private final Coder<T> trueCoder;

        public CastingCoder(Coder<T> trueCoder) {
            this.trueCoder = trueCoder;
        }

        @Override
        public void encode(KnownSize value, OutputStream outStream) throws CoderException, IOException {
            @SuppressWarnings("unchecked")
            T typedValue = (T) value;
            trueCoder.encode(typedValue, outStream);
        }

        @Override
        public KnownSize decode(InputStream inStream) throws CoderException, IOException {
            return trueCoder.decode(inStream);
        }
    }

  @Override
  protected PCollection<KnownSize> applyPrim(PCollection<Event> events) {
    return null;
  }


  @Override
  public PCollection<TimestampedValue<KnownSize>> expand(PCollection<Event> events) {

      if (configuration.debug) {
          events =
                  events
                          // Monitor events as they go by.
                          .apply(name + ".Monitor", eventMonitor.getTransform())
                          // Count each type of event.
                          .apply(name + ".Snoop", NexmarkUtils.snoop(name));
      }

      if (configuration.cpuDelayMs > 0) {
          // Slow down by pegging one core at 100%.
          events =
                  events.apply(name + ".CpuDelay", NexmarkUtils.cpuDelay(name, configuration.cpuDelayMs));
      }

      if (configuration.diskBusyBytes > 0) {
          // Slow down by forcing bytes to durable store.
          events = events.apply(name + ".DiskBusy", NexmarkUtils.diskBusy(configuration.diskBusyBytes));
      }

      // Run the query.
      // 여기부터
      return applyTyped(events);

      /*
      if (configuration.debug) {
          // Monitor results as they go by.
          queryResults = queryResults.apply(name + ".Debug", resultMonitor.getTransform());
      }

      // Timestamp the query results.
      return queryResults.apply(name + ".Stamp", NexmarkUtils.stamp(name));
      // 여기까지 따로 doFn을 만드는게?
      */
  }

  final class ToServerless extends DoFn<KV<Long, Long>,TimestampedValue<KnownSize>> {

      // connection setup
      // 1) send to serverless
      // 2) receive from serverless
      private transient AmazonKinesisClientBuilder clientBuilder;
      private transient AmazonKinesis kinesisClient;

      private transient boolean started = false;

      private static final String STREAMNAME = "nemo";

      public ToServerless() {

      }

      @ProcessElement
      public void processElement(final ProcessContext c) {
          if (!started) {
              started = true;
              this.clientBuilder = AmazonKinesisClientBuilder.standard();
              clientBuilder.setRegion("ap-northeast-1");
              this.kinesisClient = clientBuilder.build();
          }

          final PutRecordRequest putRecordRequest = new PutRecordRequest();
          putRecordRequest.setStreamName(STREAMNAME);


          PutRecordsRequest putRecordsRequest  = new PutRecordsRequest();
          putRecordsRequest.setStreamName(STREAMNAME);

          List<PutRecordsRequestEntry> putRecordsRequestEntryList  = new ArrayList<>();
          for (int i = 0; i < 1; i++) {
              PutRecordsRequestEntry putRecordsRequestEntry  = new PutRecordsRequestEntry();
              putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes()));
              putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
              putRecordsRequestEntryList.add(putRecordsRequestEntry);
          }

          putRecordsRequest.setRecords(putRecordsRequestEntryList);
          PutRecordsResult putRecordsResult  = kinesisClient.putRecords(putRecordsRequest);
          System.out.println("Put Result" + putRecordsResult);
      }
  }


}
