package io.stargate.db.cdc;

import static io.stargate.db.cdc.CDCServiceImpl.CDCConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.junit.jupiter.api.Test;

public class CDCServiceTest {
  @Test
  public void shouldNotCallProducerWhenEventNotTrackedByCDC()
      throws ExecutionException, InterruptedException {
    ServiceBuilder builder = new ServiceBuilder().withTrackingByCDC(false);
    CDCService service = builder.build();
    service.publish(mock(MutationEvent.class)).get();
    verify(builder.producer, times(0)).publish(any());
  }

  @Test
  public void shouldNotCallProducerWhenUnhealthy() {
    ServiceBuilder builder = new ServiceBuilder().withHealth(false);
    CDCService service = builder.build();

    ExecutionException runtimeEx =
        assertThrows(
            ExecutionException.class, () -> service.publish(mock(MutationEvent.class)).get());
    assertThat(runtimeEx).hasRootCauseMessage("CDC producer marked as unhealthy");

    verify(builder.producer, times(0)).publish(any());
  }

  @Test
  public void shouldCallProducerAndReportSuccess() throws ExecutionException, InterruptedException {
    ServiceBuilder builder = new ServiceBuilder();
    CDCService service = builder.build();
    service.publish(mock(MutationEvent.class)).get();
    verify(builder.producer, times(1)).publish(any());
    verify(builder.healthCheck, times(1)).reportSendSuccess();
  }

  @Test
  public void shouldReportFailureWhenProducerFails() {
    Exception ex = new Exception("Test error");
    CompletableFuture<Void> future = new CompletableFuture<>();
    future.completeExceptionally(ex);
    ServiceBuilder builder = new ServiceBuilder().withProducerFuture(future);
    CDCService service = builder.build();

    ExecutionException runtimeEx =
        assertThrows(
            ExecutionException.class, () -> service.publish(mock(MutationEvent.class)).get());
    assertThat(runtimeEx).hasCause(ex);
  }

  @Test
  public void shouldTimeoutAndReportFailure() {
    ServiceBuilder builder =
        new ServiceBuilder().withProducerFuture(new CompletableFuture<>()).withTimeout(20);
    CDCService service = builder.build();

    ExecutionException runtimeEx =
        assertThrows(
            ExecutionException.class, () -> service.publish(mock(MutationEvent.class)).get());
    assertThat(runtimeEx).hasRootCauseMessage("Timed out after 20ms");
  }

  static class ServiceBuilder {
    CDCConfig config = mock(CDCConfig.class);
    CDCProducer producer = mock(CDCProducer.class);
    CDCHealthChecker healthCheck = mock(CDCHealthChecker.class);

    ServiceBuilder() {
      withTrackingByCDC(true);
      withHealth(true);
      withProducerFuture(CompletableFuture.completedFuture(null));
    }

    ServiceBuilder withTrackingByCDC(boolean value) {
      when(config.isTrackedByCDC(any())).thenReturn(value);
      return this;
    }

    ServiceBuilder withHealth(boolean isHealthy) {
      when(healthCheck.isHealthy()).thenReturn(isHealthy);
      return this;
    }

    ServiceBuilder withProducerFuture(CompletableFuture<Void> future) {
      when(producer.publish(any())).thenReturn(future);
      return this;
    }

    ServiceBuilder withTimeout(int millis) {
      when(config.getLatencyErrorMs()).thenReturn((long) millis);
      return this;
    }

    CDCServiceImpl build() {
      return new CDCServiceImpl(producer, healthCheck, config, new MetricRegistry());
    }
  }
}
