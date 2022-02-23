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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.bridge.service.docsapi;

import io.reactivex.rxjava3.core.Single;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public final class RxUtils {

  private RxUtils() {}

  public static <E> Single<E> singleFromFuture(Supplier<CompletableFuture<E>> futureSupplier) {
    return Single.create(
        emitter ->
            futureSupplier
                .get()
                .whenComplete(
                    (e, t) -> {
                      if (null != t) {
                        emitter.onError(t);
                      } else {
                        emitter.onSuccess(e);
                      }
                    })
                .whenComplete(
                    (e, t) -> {
                      if (null != t) {
                        emitter.onError(t);
                      }
                    }));
  }
}
