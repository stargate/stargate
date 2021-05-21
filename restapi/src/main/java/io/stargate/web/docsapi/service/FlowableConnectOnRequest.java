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
import io.reactivex.rxjava3.flowables.ConnectableFlowable;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import java.util.concurrent.atomic.AtomicBoolean;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class FlowableConnectOnRequest<T> extends Flowable<T> {

  private final ConnectableFlowable<T> source;

  private FlowableConnectOnRequest(ConnectableFlowable<T> source) {
    this.source = source;
  }

  public static <T> FlowableTransformer<T, T> with() {
    return upstream -> new FlowableConnectOnRequest<>(upstream.publish());
  }

  @Override
  protected void subscribeActual(@NonNull Subscriber<? super T> subscriber) {
    source.subscribe(new OnRequestSubscription(subscriber));
  }

  private class OnRequestSubscription extends AtomicBoolean implements Subscription, Subscriber<T> {

    private final Subscriber<? super T> downstream;
    private Subscription upstream;

    private OnRequestSubscription(Subscriber<? super T> downstream) {
      this.downstream = downstream;
    }

    @Override
    public void request(long n) {
      if (compareAndSet(false, true)) {
        source.connect();
      }

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
  }
}
