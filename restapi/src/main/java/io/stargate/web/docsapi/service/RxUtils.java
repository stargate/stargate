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
package io.stargate.web.docsapi.service;

import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.concurrent.CompletableFuture;

public class RxUtils {

  public static <T> Single<T> toSingle(CompletableFuture<T> f) {
    return new Single<T>() {
      @Override
      protected void subscribeActual(SingleObserver<? super T> observer) {
        Disposable d = Disposables.empty();
        observer.onSubscribe(d);

        if (d.isDisposed()) {
          return;
        }

        f.whenComplete(
            (v, t) -> {
              if (t != null) {
                Exceptions.throwIfFatal(t);
                if (!d.isDisposed()) {
                  observer.onError(t);
                } else {
                  RxJavaPlugins.onError(t);
                }
              } else {
                if (!d.isDisposed()) {
                  observer.onSuccess(v);
                }
              }
            });
      }
    };
  }
}
