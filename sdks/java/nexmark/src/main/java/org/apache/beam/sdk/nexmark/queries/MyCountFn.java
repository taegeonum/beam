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
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.VarInt;
import java.io.*;
import java.util.Iterator;

public final class MyCountFn<T> extends Combine.CombineFn<T, long[], Long> {
  // Note that the long[] accumulator always has size 1, used as
  // a box for a mutable long.
   @Override
  public long[] createAccumulator() {
     //System.out.println("### Create accumulator");
    return new long[] {0};
  }
   @Override
  public long[] addInput(long[] accumulator, T input) {
     //System.out.println("### " + System.currentTimeMillis() + " Add input: " + input);
     accumulator[0] += 1;
    return accumulator;
  }
   @Override
  public long[] mergeAccumulators(Iterable<long[]> accumulators) {
    Iterator<long[]> iter = accumulators.iterator();
    if (!iter.hasNext()) {
      return createAccumulator();
    }
    long[] running = iter.next();
    while (iter.hasNext()) {
      running[0] += iter.next()[0];
    }
    System.out.println("### Merging time: " + System.currentTimeMillis() + ", result: " + running);
    return running;
  }
   @Override
  public Long extractOutput(long[] accumulator) {
    return accumulator[0];
  }
   @Override
  public Coder<long[]> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
    return new AtomicCoder<long[]>() {
      @Override
      public void encode(long[] value, OutputStream outStream) throws IOException {
        VarInt.encode(value[0], outStream);
      }
       @Override
      public long[] decode(InputStream inStream) throws IOException, CoderException {
        try {
          return new long[] {VarInt.decodeLong(inStream)};
        } catch (EOFException | UTFDataFormatException exn) {
          throw new CoderException(exn);
        }
      }
       @Override
      public boolean isRegisterByteSizeObserverCheap(long[] value) {
        return true;
      }
       @Override
      protected long getEncodedElementByteSize(long[] value) {
        return VarInt.getLength(value[0]);
      }
    };
  }
  @Override
  public boolean equals(Object other) {
    return other != null && getClass().equals(other.getClass());
  }
   @Override
  public int hashCode() {
    return getClass().hashCode();
  }
} 