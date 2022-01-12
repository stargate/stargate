/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.common.grpc;

import io.grpc.stub.StreamObserver;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A stream observer that starts its life accumulating incoming elements; it only starts streaming
 * them (via {@link #onNextReady(Object)} once {@link #markReady()} is called.
 */
public abstract class InitReplayingStreamObserver<T> implements StreamObserver<T> {

  // This is based on the "queue-drain" pattern
  // https://github.com/ReactiveX/RxJava/wiki/Implementing-custom-operators-(draft)#the-queue-drain-approach
  private final Queue<T> elements = new ConcurrentLinkedQueue<>();
  private final AtomicReference<ObserverState> state = new AtomicReference<>(ObserverState.INITIAL);

  /**
   * What to do when we're ready to process an element (either because we had accumulated it and the
   * stream just became ready, or we just received it and the stream was already ready).
   */
  protected abstract void onNextReady(T element);

  @Override
  public final void onNext(T element) {
    elements.offer(element);
    ObserverState previousState = state.getAndUpdate(ObserverState::increment);
    if (previousState.ready && previousState.count == 0) {
      drain();
    }
  }

  public void markReady() {
    ObserverState previousState = state.getAndUpdate(ObserverState::markReady);
    if (previousState.ready) {
      throw new IllegalStateException("markReady can only be called once");
    }
    if (previousState.count > 0) {
      drain();
    }
  }

  private void drain() {
    do {
      T element = elements.poll();
      assert element != null;
      onNextReady(element);
    } while (state.updateAndGet(ObserverState::decrement).count != 0);
  }

  private static class ObserverState {

    private static final ObserverState INITIAL = new ObserverState(0, false);

    final int count;
    final boolean ready;

    private ObserverState(int count, boolean ready) {
      this.count = count;
      this.ready = ready;
    }

    ObserverState increment() {
      return new ObserverState(count + 1, ready);
    }

    ObserverState decrement() {
      assert count > 0;
      return new ObserverState(count - 1, ready);
    }

    ObserverState markReady() {
      return new ObserverState(count, true);
    }
  }
}
