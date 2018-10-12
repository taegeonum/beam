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
package org.apache.beam.runners.flink.translation.functions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.functions.RuntimeContext;

/** A {@link SideInputReader} for the Flink Batch Runner. */
public class FlinkSideInputReader implements SideInputReader {
  /** A {@link MultimapView} which always returns an empty iterable. */
  private static final MultimapView EMPTY_MULTMAP_VIEW = o -> Collections.EMPTY_LIST;

  private final Map<TupleTag<?>, WindowingStrategy<?, ?>> sideInputs;

  private RuntimeContext runtimeContext;

  private final Map<BoundedWindow, Tup> windowMap;

  private boolean started = false;
  private transient ScheduledExecutorService scheduledExecutorService;

  public FlinkSideInputReader(
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> indexByView, RuntimeContext runtimeContext) {
    for (PCollectionView<?> view : indexByView.keySet()) {
      checkArgument(
          Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
              view.getViewFn().getMaterialization().getUrn()),
          "This handler is only capable of dealing with %s materializations "
              + "but was asked to handle %s for PCollectionView with tag %s.",
          Materializations.MULTIMAP_MATERIALIZATION_URN,
          view.getViewFn().getMaterialization().getUrn(),
          view.getTagInternal().getId());
    }
    windowMap = new HashMap<>();
    sideInputs = new HashMap<>();
    for (Map.Entry<PCollectionView<?>, WindowingStrategy<?, ?>> entry : indexByView.entrySet()) {
      sideInputs.put(entry.getKey().getTagInternal(), entry.getValue());
    }
    this.runtimeContext = runtimeContext;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    if (!started) {
      started = true;
      scheduledExecutorService = Executors.newScheduledThreadPool(1);
      scheduledExecutorService.scheduleAtFixedRate(() -> {
        for (final BoundedWindow w : windowMap.keySet()) {
          System.out.println("Window " + w + ", latency: " + (windowMap.get(w).end - windowMap.get(w).start));
        }
      }, 5, 5, TimeUnit.SECONDS);
    }

    checkNotNull(view, "View passed to sideInput cannot be null");
    TupleTag<?> tag = view.getTagInternal();
    checkNotNull(sideInputs.get(tag), "Side input for " + view + " not available.");

    Map<BoundedWindow, T> sideInputs =
        runtimeContext.getBroadcastVariableWithInitializer(
            tag.getId(), new SideInputInitializer<>(view));
    T result = sideInputs.get(window);

    System.out.println(System.currentTimeMillis()  + " Flink get sideinput: " + result);
    if (result == null) {
      ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
      result = viewFn.apply(EMPTY_MULTMAP_VIEW);
    }

    final Tup t = windowMap.get(window);
    if (t == null) {
      windowMap.put(window, new Tup(System.currentTimeMillis(), System.currentTimeMillis()));
    } else {
      t.end = System.currentTimeMillis();
    }
    return result;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  final class Tup implements Serializable {
    long end;
    long start;
    public Tup(long start, long end) {
      this.start = start;
      this.end = end;
    }
  }
}
