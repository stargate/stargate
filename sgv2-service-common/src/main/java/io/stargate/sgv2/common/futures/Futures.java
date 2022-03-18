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
package io.stargate.sgv2.common.futures;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class Futures {

  /**
   * Gets a future without reacting to interruptions: if the current thread is interrupted, the
   * operation continues until the future completes, and the thread will be re-interrupted upon
   * completion.
   *
   * <p>If the future fails with an unchecked exception, it will be rethrown as-is. If it fails with
   * a checked exception, it will be wrapped in an {@link UncheckedExecutionException}.
   */
  public static <T> T getUninterruptibly(CompletionStage<T> stage) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return stage.toCompletableFuture().get();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof RuntimeException) {
            throw ((RuntimeException) cause);
          } else if (cause instanceof Error) {
            throw ((Error) cause);
          } else {
            throw new UncheckedExecutionException(cause);
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static <T> CompletableFuture<T> failedFuture(Exception e) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(e);
    return f;
  }
}
