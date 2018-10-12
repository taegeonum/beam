package org.apache.beam.sdk.nexmark.queries;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.VarInt;

import java.io.*;
import java.util.Iterator;

/** A {@link CombineFn} that counts elements. */
public final class MyCountFn<T> extends Combine.CombineFn<T, long[], Long> {
  // Note that the long[] accumulator always has size 1, used as
  // a box for a mutable long.

  @Override
  public long[] createAccumulator() {
    return new long[] {0};
  }

  @Override
  public long[] addInput(long[] accumulator, T input) {
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