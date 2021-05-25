package io.stargate.grpc.service.interceptors.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.InputStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class MetricCollectingServerInterceptorTest {

  MeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Nested
  class RequestCounter {

    @Test
    public void happyPath() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onMessage("message");
      objectListener.onComplete();

      Counter counter = meterRegistry.get("grpc.server.requests.received").counter();
      assertThat(counter.count()).isEqualTo(1.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void messageNotSent() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onComplete();

      Counter counter = meterRegistry.get("grpc.server.requests.received").counter();
      assertThat(counter.count()).isEqualTo(0.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void callNotCompleted() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onMessage("message");

      Counter counter = meterRegistry.get("grpc.server.requests.received").counter();
      assertThat(counter.count()).isEqualTo(1.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void onCancel() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onCancel();

      Counter counter = meterRegistry.get("grpc.server.requests.received").counter();
      assertThat(counter.count()).isEqualTo(0.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }
  }

  @Nested
  class ResponseCounter {

    @Test
    public void happyPath() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onMessage("message");
      objectListener.onComplete();

      Counter counter = meterRegistry.get("grpc.server.responses.sent").counter();
      assertThat(counter.count()).isEqualTo(1.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void messageNotSent() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onComplete();

      Counter counter = meterRegistry.get("grpc.server.responses.sent").counter();
      assertThat(counter.count()).isEqualTo(0.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void callNotCompleted() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onMessage("message");

      Counter counter = meterRegistry.get("grpc.server.responses.sent").counter();
      assertThat(counter.count()).isEqualTo(1.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void onCancel() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onCancel();

      Counter counter = meterRegistry.get("grpc.server.responses.sent").counter();
      assertThat(counter.count()).isEqualTo(0.0);
      assertThat(counter.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }
  }

  @Nested
  class TimerFunction {

    @Test
    public void happyPath() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onComplete();

      Timer timer =
          meterRegistry.get("grpc.server.processing.duration").tag("statusCode", "UNKNOWN").timer();
      assertThat(timer.count()).isEqualTo(1L);
      assertThat(timer.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void messageNotSent() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onComplete();

      Timer timer =
          meterRegistry.get("grpc.server.processing.duration").tag("statusCode", "UNKNOWN").timer();
      assertThat(timer.count()).isEqualTo(1L);
      assertThat(timer.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }

    @Test
    public void callNotCompleted() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onMessage("foo");

      assertThat(
              meterRegistry
                  .find("grpc.server.processing.duration")
                  .tag("statusCode", "UNKNOWN")
                  .timer())
          .isNull();
    }

    @Test
    public void onCancel() {
      meterRegistry.clear();
      MetricCollectingServerInterceptor interceptor =
          new MetricCollectingServerInterceptor(meterRegistry);
      Listener<Object> objectListener =
          interceptor.interceptCall(
              new TestServerCall(), new Metadata(), new TestServerCallHandler());

      objectListener.onCancel();

      Timer timer =
          meterRegistry
              .get("grpc.server.processing.duration")
              .tag("statusCode", "CANCELLED")
              .timer();
      assertThat(timer.count()).isEqualTo(1L);
      assertThat(timer.getId().getTags())
          .contains(
              Tag.of("method", "bar"), Tag.of("methodType", "UNARY"), Tag.of("service", "foo"));
    }
  }

  static class TestServerCallHandler implements ServerCallHandler<Object, Object> {

    @Override
    public Listener<Object> startCall(ServerCall<Object, Object> serverCall, Metadata metadata) {
      return new Listener<Object>() {
        @Override
        public void onMessage(Object message) {
          serverCall.sendMessage(message);
        }
      };
    }
  }

  static class TestServerCall extends ServerCall<Object, Object> {

    private final MethodDescriptor<Object, Object> method;

    public TestServerCall() {
      this.method =
          MethodDescriptor.newBuilder()
              .setFullMethodName("foo/bar")
              .setType(MethodType.UNARY)
              .setRequestMarshaller(new TestMarshaller())
              .setResponseMarshaller(new TestMarshaller())
              .build();
    }

    @Override
    public void request(int i) {}

    @Override
    public void sendHeaders(Metadata metadata) {}

    @Override
    public void sendMessage(Object o) {}

    @Override
    public void close(Status status, Metadata metadata) {}

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public MethodDescriptor<Object, Object> getMethodDescriptor() {
      return this.method;
    }
  }

  static class TestMarshaller implements Marshaller<Object> {

    @Override
    public InputStream stream(Object o) {
      return null;
    }

    @Override
    public Object parse(InputStream inputStream) {
      return null;
    }
  }
}
