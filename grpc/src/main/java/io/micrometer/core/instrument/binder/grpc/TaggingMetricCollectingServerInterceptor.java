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

package io.micrometer.core.instrument.binder.grpc;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.stargate.grpc.metrics.api.GrpcMetricsTagProvider;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Extension of the micrometer's {@link MetricCollectingServerInterceptor} that enables us to add
 * extra tags on each {@link ServerCall}.
 */
public class TaggingMetricCollectingServerInterceptor extends MetricCollectingServerInterceptor {

  /**
   * The total number of requests received, same as
   * MetricCollectingServerInterceptor#METRIC_NAME_SERVER_REQUESTS_RECEIVED.
   */
  private static final String METRIC_NAME_SERVER_REQUESTS_RECEIVED =
      "grpc.server.requests.received";

  /**
   * The total number of responses sent, same as
   * MetricCollectingServerInterceptor#METRIC_NAME_SERVER_RESPONSES_SENT.
   */
  private static final String METRIC_NAME_SERVER_RESPONSES_SENT = "grpc.server.responses.sent";

  /**
   * The total time taken for the server to complete the call, same as
   * MetricCollectingServerInterceptor#METRIC_NAME_SERVER_PROCESSING_DURATION.
   */
  private static final String METRIC_NAME_SERVER_PROCESSING_DURATION =
      "grpc.server.processing.duration";

  /** Stargate {@link GrpcMetricsTagProvider} for extra tags on each call. */
  private final GrpcMetricsTagProvider tagProvider;

  public TaggingMetricCollectingServerInterceptor(
      MeterRegistry registry, GrpcMetricsTagProvider tagProvider) {
    super(registry);
    this.tagProvider = tagProvider;
  }

  public TaggingMetricCollectingServerInterceptor(
      MeterRegistry registry,
      UnaryOperator<Counter.Builder> counterCustomizer,
      UnaryOperator<Timer.Builder> timerCustomizer,
      GrpcMetricsTagProvider tagProvider,
      Status.Code... eagerInitializedCodes) {
    super(registry, counterCustomizer, timerCustomizer, eagerInitializedCodes);
    this.tagProvider = tagProvider;
  }

  @Override
  public <Q, A> ServerCall.Listener<Q> interceptCall(
      ServerCall<Q, A> call, Metadata requestHeaders, ServerCallHandler<Q, A> next) {
    // only first line changed from the super impl
    final MetricSet metrics = metricsFor(call);
    final Consumer<Status.Code> responseStatusTiming =
        metrics.newProcessingDurationTiming(this.registry);

    final MetricCollectingServerCall<Q, A> monitoringCall =
        new MetricCollectingServerCall<>(call, metrics.getResponseCounter());

    return new MetricCollectingServerCallListener<>(
        next.startCall(monitoringCall, requestHeaders),
        metrics.getRequestCounter(),
        monitoringCall::getResponseCode,
        responseStatusTiming);
  }

  private MetricSet metricsFor(ServerCall<?, ?> call) {
    MethodDescriptor<?, ?> method = call.getMethodDescriptor();
    Tags callTags = tagProvider.getCallTags(call);

    // check if there is any extension, if not return super
    // otherwise use our implementation
    if (Tags.empty().equals(callTags)) {
      return super.metricsFor(method);
    } else {
      return new MetricSet(
          newRequestCounterFor(method, callTags),
          newResponseCounterFor(method, callTags),
          newTimerFunction(method, callTags));
    }
  }

  // copied from super, extending with Tags
  private Counter newRequestCounterFor(final MethodDescriptor<?, ?> method, Tags tags) {
    return this.counterCustomizer
        .apply(
            prepareCounterFor(
                method,
                METRIC_NAME_SERVER_REQUESTS_RECEIVED,
                "The total number of requests received"))
        .tags(tags)
        .register(this.registry);
  }

  // copied from super, extending with Tags
  private Counter newResponseCounterFor(final MethodDescriptor<?, ?> method, Tags tags) {
    return this.counterCustomizer
        .apply(
            prepareCounterFor(
                method, METRIC_NAME_SERVER_RESPONSES_SENT, "The total number of responses sent"))
        .tags(tags)
        .register(this.registry);
  }

  // copied from super, extending with Tags
  private Function<Status.Code, Timer> newTimerFunction(
      final MethodDescriptor<?, ?> method, Tags tags) {
    return asTimerFunction(
        () ->
            this.timerCustomizer.apply(
                prepareTimerFor(
                        method,
                        METRIC_NAME_SERVER_PROCESSING_DURATION,
                        "The total time taken for the server to complete the call")
                    .tags(tags)));
  }
}
