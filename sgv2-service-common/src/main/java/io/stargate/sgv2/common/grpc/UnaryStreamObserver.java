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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class UnaryStreamObserver<T> implements StreamObserver<T> {

  private final CompletableFuture<T> future = new CompletableFuture<>();

  @Override
  public void onNext(T value) {
    if (!future.complete(value)) {
      throw illegalState();
    }
  }

  @Override
  public void onError(Throwable t) {
    if (!future.completeExceptionally(t)) {
      throw illegalState();
    }
  }

  @Override
  public void onCompleted() {
    // intentionally empty
  }

  public CompletionStage<T> asFuture() {
    return future;
  }

  private IllegalStateException illegalState() {
    return new IllegalStateException("onNext/onError should be called only once");
  }
}
