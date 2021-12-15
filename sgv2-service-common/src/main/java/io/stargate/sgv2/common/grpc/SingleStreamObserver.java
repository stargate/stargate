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
import java.util.function.Consumer;

/**
 * An observer for an async gRPC operation that is known to return exactly one result.
 *
 * <p>It can be converted to a Java future for better composability.
 */
public class SingleStreamObserver<T> implements StreamObserver<T> {

  /**
   * Captures the common pattern of creating an observer, passing it to a gRPC operation, and
   * converting it to a future.
   */
  public static <T> CompletionStage<T> toFuture(Consumer<StreamObserver<T>> operation) {
    SingleStreamObserver<T> observer = new SingleStreamObserver<>();
    operation.accept(observer);
    return observer.toFuture();
  }

  private final CompletableFuture<T> future = new CompletableFuture<>();

  @Override
  public void onNext(T t) {
    assert !future.isDone();
    future.complete(t);
  }

  @Override
  public void onError(Throwable throwable) {
    assert !future.isDone();
    future.completeExceptionally(throwable);
  }

  @Override
  public void onCompleted() {
    assert future.isDone();
  }

  public CompletionStage<T> toFuture() {
    return future;
  }
}
