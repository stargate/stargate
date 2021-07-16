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
package io.stargate.graphql.schema.graphqlfirst.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class CompletableFutures {

  public static <T> CompletableFuture<T> failedFuture(Throwable cause) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(cause);
    return future;
  }

  /** Returns a future of all the results, or a failed future if any of the inputs fails. */
  public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
        .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }
}
