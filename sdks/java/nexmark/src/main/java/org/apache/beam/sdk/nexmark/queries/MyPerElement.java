package org.apache.beam.sdk.nexmark.queries;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
*
* @param <T> the type of the elements of the input {@code PCollection}, and the type of the keys
*     of the output {@code PCollection}
*/
public final class MyPerElement<T> extends PTransform<PCollection<T>, PCollection<KV<T, Long>>> {
     public MyPerElement() {
    }
     @Override
    public PCollection<KV<T, Long>> expand(PCollection<T> input) {
        return input
                .apply(
                        "Init",
                        MapElements.via(
                                new SimpleFunction<T, KV<T, Integer>>() {
                                    @Override
                                    public KV<T, Integer> apply(T element) {
                                        final Long l = (Long) element;
                                        final T index = (T) ((Long) (l % 100));
                                        return KV.of(index, 1);
                                    }
                                }))
                .apply(Count.perKey());
        /*
                .apply(GroupByKey.create())
                .apply("mycombine",
                        ParDo.of(new DoFn<KV<T, Iterable<Integer>>, KV<T, Long>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                System.out.println(c.element().getKey() + ": " + c.element().getValue());
                            }
                        }));
                        */
    }
}