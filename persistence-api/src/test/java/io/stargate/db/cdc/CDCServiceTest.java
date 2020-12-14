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
package io.stargate.db.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.codahale.metrics.MetricRegistry;
import io.stargate.db.cdc.config.CDCConfig;
import io.stargate.db.schema.Table;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.exceptions.CDCWriteException;
import org.junit.jupiter.api.Test;

public class CDCServiceTest {
  @Test
  public void shouldNotCallProducerWhenEventNotTrackedByCDC()
      throws ExecutionException, InterruptedException {
    ServiceBuilder builder = new ServiceBuilder().withTrackingByCDC(false);
    CDCService service = builder.build();
    service.publish(mockMutationEvent()).get();
    verify(builder.producer, times(0)).publish(any());
  }

  @Test
  public void shouldNotCallProducerWhenUnhealthy() {
    ServiceBuilder builder = new ServiceBuilder().withHealth(false);
    CDCService service = builder.build();

    ExecutionException runtimeEx =
        assertThrows(ExecutionException.class, () -> service.publish(mockMutationEvent()).get());
    assertThat(runtimeEx).hasRootCauseMessage("CDC producer marked as unhealthy");

    verify(builder.producer, times(0)).publish(any());
  }

  @Test
  public void shouldCallProducerAndReportSuccess() throws ExecutionException, InterruptedException {
    ServiceBuilder builder = new ServiceBuilder();
    CDCService service = builder.build();
    service.publish(mockMutationEvent()).get();
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
        assertThrows(ExecutionException.class, () -> service.publish(mockMutationEvent()).get());
    assertThat(runtimeEx).hasCauseInstanceOf(CDCWriteException.class);
    assertThat(runtimeEx.getCause()).hasCause(ex);
  }

  @Test
  public void shouldTimeoutAndReportFailure() {
    ServiceBuilder builder =
        new ServiceBuilder().withProducerFuture(new CompletableFuture<>()).withTimeout(20);
    CDCService service = builder.build();

    ExecutionException runtimeEx =
        assertThrows(ExecutionException.class, () -> service.publish(mockMutationEvent()).get());
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
      when(config.getProducerTimeoutMs()).thenReturn((long) millis);
      return this;
    }

    CDCServiceImpl build() {
      return new CDCServiceImpl(producer, healthCheck, config, new MetricRegistry());
    }
  }

  private MutationEvent mockMutationEvent() {
    MutationEvent mutationEvent = mock(MutationEvent.class);
    Table table = mock(Table.class);
    when(mutationEvent.getTable()).thenReturn(table);
    return mutationEvent;
  }
}
