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

package io.stargate.web.docsapi.resources.async;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.SingleObserver;
import io.reactivex.rxjava3.disposables.Disposable;
import java.io.IOException;
import java.util.function.Function;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.ws.rs.container.AsyncResponse;

/**
 * Simple async observer that works with the async response. If {@link #errorHandler} is provided
 * any error is trasnformed to the <V> and supplied to the async response. Otherwise the throwable
 * is supplied.
 *
 * @param <V> Type of the result
 */
public class AsyncObserver<V> implements SingleObserver<V> {

  private final AsyncResponse asyncResponse;

  private final Function<Throwable, V> errorHandler;

  private AsyncObserver(AsyncResponse asyncResponse, Function<Throwable, V> errorHandler) {
    this.asyncResponse = asyncResponse;
    this.errorHandler = errorHandler;
  }

  public static <M> AsyncObserver<M> forResponse(AsyncResponse asyncResponse) {
    return new AsyncObserver<>(asyncResponse, null);
  }

  public static <M> AsyncObserver<M> forResponseWithHandler(
      AsyncResponse asyncResponse, Function<Throwable, M> errorHandler) {
    return new AsyncObserver<>(asyncResponse, errorHandler);
  }

  @Override
  public void onSubscribe(@NonNull Disposable d) {
    // if already canceled, dispose
    // otherwise register the listener
    if (asyncResponse.isCancelled()) {
      d.dispose();
    } else {
      asyncResponse.register(new DisposableAsyncListener(d));
    }
  }

  @Override
  public void onSuccess(@NonNull V v) {
    asyncResponse.resume(v);
  }

  @Override
  public void onError(@NonNull Throwable e) {
    if (null != errorHandler) {
      asyncResponse.resume(errorHandler.apply(e));
    } else {
      asyncResponse.resume(e);
    }
  }

  // make sure that disposable is disposed on timeout
  private static class DisposableAsyncListener implements AsyncListener {

    private final Disposable disposable;

    public DisposableAsyncListener(Disposable disposable) {
      this.disposable = disposable;
    }

    @Override
    public void onComplete(AsyncEvent asyncEvent) throws IOException {
      // no action possible
    }

    @Override
    public void onTimeout(AsyncEvent asyncEvent) throws IOException {
      disposable.dispose();
    }

    @Override
    public void onError(AsyncEvent asyncEvent) throws IOException {
      // no action possible
    }

    @Override
    public void onStartAsync(AsyncEvent asyncEvent) throws IOException {
      // no action possible
    }
  }
}
