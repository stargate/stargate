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

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableTransformer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Subscribes to the given {@link ConnectableFlowable} but connects only when downstream subscribers
 * {@link Subscription#request(long) request} elements.
 *
 * <p>Note: upstream {@link Flowable} is never cancelled.
 *
 * @see #with()
 */
public class FlowableConnectOnRequest<T> extends Flowable<T> {

  private final ConnectableFlowable<T> source;

  private FlowableConnectOnRequest(ConnectableFlowable<T> source) {
    this.source = source;
  }

  /**
   * Creates a {@link Flowable#publish() publisher} from the given {@link Flowable} and wraps it in
   * a {@link FlowableConnectOnRequest} instance.
   */
  public static <T> FlowableTransformer<T, T> with() {
    return upstream -> new FlowableConnectOnRequest<>(upstream.publish());
  }

  @Override
  protected void subscribeActual(@NonNull Subscriber<? super T> subscriber) {
    source.subscribe(new OnRequestSubscription(subscriber));
  }

  private class OnRequestSubscription
      implements Subscription, Subscriber<T>, @NonNull Consumer<Disposable> {

    private final Subscriber<? super T> downstream;
    private Subscription upstream;

    private OnRequestSubscription(Subscriber<? super T> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void request(long n) {
      // We connect only when a requests comes. Also, ConnectableFuture impl. is expected to
      // handle concurrent and repeated connect calls.
      // `this` receives the connection's Disposable - see accept(...). We use an explicit
      // Consumer<Disposable> here to avoid creating extra Consumer objects on every call.
      source.connect(this);

      upstream.request(n);
    }

    @Override
    public void cancel() {
      upstream.cancel();
    }

    @Override
    public void onSubscribe(Subscription s) {
      if (SubscriptionHelper.validate(upstream, s)) {
        upstream = s;
        downstream.onSubscribe(this);
      }
    }

    @Override
    public void onNext(T value) {
      downstream.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      downstream.onError(t);
    }

    @Override
    public void onComplete() {
      downstream.onComplete();
    }

    @Override
    public void accept(Disposable disposable) throws Throwable {
      // nop - we do not cancel Flowable connections (cf. request(int))
    }
  }
}
