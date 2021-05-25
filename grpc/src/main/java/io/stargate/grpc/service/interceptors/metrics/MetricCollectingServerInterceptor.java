/*
 * Copyright 2021 VMware, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc.service.interceptors.metrics;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.Builder;
import io.micrometer.core.instrument.Timer.Sample;
import io.micrometer.core.instrument.binder.BaseUnits;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * A gRPC server interceptor that will collect metrics using the given {@link MeterRegistry}.
 *
 * <p><b>Usage:</b>
 *
 * <pre>
 * Server server = ServerBuilder.forPort(8080)
 *         .intercept(new MetricCollectingServerInterceptor(meterRegistry))
 *         .build();
 *
 * server.start()
 * </pre>
 *
 * @author Daniel Theuke (daniel.theuke@heuboe.de)
 * @since 1.7.0
 */
public class MetricCollectingServerInterceptor implements ServerInterceptor {

  /** The total number of requests received */
  private static final String METRIC_NAME_SERVER_REQUESTS_RECEIVED =
      "grpc.server.requests.received";
  /** The total number of responses sent */
  private static final String METRIC_NAME_SERVER_RESPONSES_SENT = "grpc.server.responses.sent";
  /** The total time taken for the server to complete the call. */
  private static final String METRIC_NAME_SERVER_PROCESSING_DURATION =
      "grpc.server.processing.duration";
  /** The metrics tag key that belongs to the called service name. */
  private static final String TAG_SERVICE_NAME = "service";
  /** The metrics tag key that belongs to the called method name. */
  private static final String TAG_METHOD_NAME = "method";
  /** The metrics tag key that belongs to the type of the called method. */
  private static final String TAG_METHOD_TYPE = "methodType";
  /** The metrics tag key that belongs to the result status code. */
  private static final String TAG_STATUS_CODE = "statusCode";

  private final Map<MethodDescriptor<?, ?>, MetricSet> metricsForMethods =
      new ConcurrentHashMap<>();

  private final MeterRegistry registry;

  private final UnaryOperator<Counter.Builder> counterCustomizer;
  private final UnaryOperator<Timer.Builder> timerCustomizer;
  private final Status.Code[] eagerInitializedCodes;

  /**
   * Creates a new gRPC server interceptor that will collect metrics into the given {@link
   * MeterRegistry}.
   *
   * @param registry The registry to use.
   */
  public MetricCollectingServerInterceptor(final MeterRegistry registry) {
    this.registry = registry;
    this.counterCustomizer = UnaryOperator.identity();
    this.timerCustomizer = UnaryOperator.identity();
    this.eagerInitializedCodes = new Code[] {Code.OK};
  }

  /**
   * Gets or creates a {@link MetricSet} for the given gRPC method. This will initialize all default
   * counters and timers for that method.
   *
   * @param method The method to get the metric set for.
   * @return The metric set for the given method.
   * @see #newMetricsFor(MethodDescriptor)
   */
  protected final MetricSet metricsFor(final MethodDescriptor<?, ?> method) {
    // TODO: [doug] 2021-05-25, Tue, 11:44 Key based on method and headers
    return this.metricsForMethods.computeIfAbsent(method, this::newMetricsFor);
  }

  /**
   * Creates a {@link MetricSet} for the given gRPC method. This will initialize all default
   * counters and timers for that method.
   *
   * @param method The method to get the metric set for.
   * @return The newly created metric set for the given method.
   */
  protected MetricSet newMetricsFor(final MethodDescriptor<?, ?> method) {
    return new MetricSet(
        newRequestCounterFor(method), newResponseCounterFor(method), newTimerFunction(method));
  }

  protected Counter newRequestCounterFor(final MethodDescriptor<?, ?> method) {
    return this.counterCustomizer
        .apply(
            prepareCounterFor(
                method,
                METRIC_NAME_SERVER_REQUESTS_RECEIVED,
                "The total number of requests received"))
        .register(this.registry);
  }

  /**
   * Creates a new counter builder for the given method. By default the base unit will be messages.
   *
   * @param method The method the counter will be created for.
   * @param name The name of the counter to use.
   * @param description The description of the counter to use.
   * @return The newly created counter builder.
   */
  protected static Counter.Builder prepareCounterFor(
      final MethodDescriptor<?, ?> method, final String name, final String description) {
    return Counter.builder(name)
        .description(description)
        .baseUnit(BaseUnits.MESSAGES)
        .tags(requestTags(method));
  }

  protected Counter newResponseCounterFor(final MethodDescriptor<?, ?> method) {
    return this.counterCustomizer
        .apply(
            prepareCounterFor(
                method, METRIC_NAME_SERVER_RESPONSES_SENT, "The total number of responses sent"))
        .register(this.registry);
  }

  protected Function<Code, Timer> newTimerFunction(final MethodDescriptor<?, ?> method) {
    return asTimerFunction(
        () ->
            this.timerCustomizer.apply(
                prepareTimerFor(
                    method,
                    METRIC_NAME_SERVER_PROCESSING_DURATION,
                    "The total time taken for the server to complete the call")));
  }

  /**
   * Creates a new timer function using the given template. This method initializes the default
   * timers.
   *
   * @param timerTemplate The template to create the instances from.
   * @return The newly created function that returns a timer for a given code.
   */
  @SuppressWarnings("ReturnValueIgnored")
  protected Function<Code, Timer> asTimerFunction(final Supplier<Builder> timerTemplate) {
    final Map<Code, Timer> cache = new EnumMap<>(Code.class);
    final Function<Code, Timer> creator =
        code -> timerTemplate.get().tag(TAG_STATUS_CODE, code.name()).register(this.registry);
    final Function<Code, Timer> cacheResolver = code -> cache.computeIfAbsent(code, creator);
    // Eager initialize
    for (final Code code : this.eagerInitializedCodes) {
      cacheResolver.apply(code);
    }
    return cacheResolver;
  }

  /**
   * Creates a new timer builder for the given method.
   *
   * @param method The method the timer will be created for.
   * @param name The name of the timer to use.
   * @param description The description of the timer to use.
   * @return The newly created timer builder.
   */
  protected static Timer.Builder prepareTimerFor(
      final MethodDescriptor<?, ?> method, final String name, final String description) {
    return Timer.builder(name).description(description).tags(requestTags(method));
  }

  private static Iterable<Tag> requestTags(MethodDescriptor<?, ?> method) {
    if (method == null) {
      return Tags.empty();
    }

    // TODO: [doug] 2021-05-25, Tue, 11:45 Use io.stargate.core.metrics.api.HttpMetricsTagProvider
    // or create new impl
    return Tags.of(
        TAG_SERVICE_NAME, method.getServiceName(),
        TAG_METHOD_NAME, method.getBareMethodName(),
        TAG_METHOD_TYPE, method.getType().name());
  }

  @Override
  public <Q, A> ServerCall.Listener<Q> interceptCall(
      final ServerCall<Q, A> call,
      final Metadata requestHeaders,
      final ServerCallHandler<Q, A> next) {

    // TODO: [doug] 2021-05-25, Tue, 11:49 Pass headers or tagProvider
    final MetricSet metrics = metricsFor(call.getMethodDescriptor());
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

  /** Container for all metrics of a certain call. Used instead of 3 maps to improve performance. */
  protected static class MetricSet {

    private final Counter requestCounter;
    private final Counter responseCounter;
    private final Function<Code, Timer> timerFunction;

    /**
     * Creates a new metric set with the given meter instances.
     *
     * @param requestCounter The request counter to use.
     * @param responseCounter The response counter to use.
     * @param timerFunction The timer function to use.
     */
    public MetricSet(
        final Counter requestCounter,
        final Counter responseCounter,
        final Function<Code, Timer> timerFunction) {

      this.requestCounter = requestCounter;
      this.responseCounter = responseCounter;
      this.timerFunction = timerFunction;
    }

    /**
     * Gets the Counter that counts the request messages.
     *
     * @return The Counter that counts the request messages.
     */
    public Counter getRequestCounter() {
      return this.requestCounter;
    }

    /**
     * Gets the Counter that counts the response messages.
     *
     * @return The Counter that counts the response messages.
     */
    public Counter getResponseCounter() {
      return this.responseCounter;
    }

    /**
     * Uses the given registry to create a {@link Sample Timer.Sample} that will be reported if the
     * returned consumer is invoked.
     *
     * @param registry The registry used to create the sample.
     * @return The newly created consumer that will report the processing duration since calling
     *     this method and invoking the returned consumer along with the status code.
     */
    public Consumer<Status.Code> newProcessingDurationTiming(final MeterRegistry registry) {
      final Timer.Sample timerSample = Timer.start(registry);
      return code -> timerSample.stop(this.timerFunction.apply(code));
    }
  }
}
