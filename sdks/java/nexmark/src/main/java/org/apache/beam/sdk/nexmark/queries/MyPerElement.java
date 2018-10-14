package org.apache.beam.sdk.nexmark.queries;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
                                new SimpleFunction<T, KV<T, Void>>() {
                                    @Override
                                    public KV<T, Void> apply(T element) {
                                        System.out.println("hello: " + element);
                                        return KV.of(element, (Void) null);
                                    }
                                }))
                .apply(Combine.perKey(new MyCountFn<>()));
    }
} 