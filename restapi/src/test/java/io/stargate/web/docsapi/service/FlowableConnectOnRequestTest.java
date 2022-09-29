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

import static org.assertj.core.api.Assertions.assertThat;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import org.junit.jupiter.api.Test;

class FlowableConnectOnRequestTest {

  @Test
  void testNoDataWithoutRequest() {
    TestSubscriber<String> test =
        Flowable.just("a").compose(FlowableConnectOnRequest.with()).test(0);
    test.assertNotComplete();
    assertThat(test.values()).isEmpty();
  }

  @Test
  void testOnNext() {
    Flowable.just("a").compose(FlowableConnectOnRequest.with()).test(1).assertValue("a");
  }

  @Test
  void testMultipleRequests() {
    TestSubscriber<String> test =
        Flowable.just("a", "b", "c").compose(FlowableConnectOnRequest.with()).test(0);
    test.request(1);
    test.assertValues("a");
    test.request(2);
    test.assertValues("a", "b", "c");
  }

  @Test
  void testOnComplete() {
    Flowable.just("a").compose(FlowableConnectOnRequest.with()).test(1).assertComplete();
  }

  @Test
  void testOnError() {
    Flowable.error(new IllegalStateException("test-exception"))
        .compose(FlowableConnectOnRequest.with())
        .test(1)
        .assertError(t -> t.getMessage().equals("test-exception"));
  }

  @Test
  void testCancel() {
    TestSubscriber<String> testSubscriber =
        Flowable.just("a").compose(FlowableConnectOnRequest.with()).test(0);
    testSubscriber.cancel();
    testSubscriber.request(1); // not effective due to cancel() above
    testSubscriber.assertNotComplete();
    assertThat(testSubscriber.values()).isEmpty();
  }
}
