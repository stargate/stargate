package io.stargate.db.metrics.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.db.ClientInfo;
import io.stargate.db.DriverInfo;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class ClientInfoMetricsTagProviderTest {

  @Nested
  class GetCallTags {

    @Test
    public void cql() {
      ClientInfo clientInfo = mock(ClientInfo.class);
      DriverInfo driverInfo = mock(DriverInfo.class);
      when(driverInfo.name()).thenReturn("driver1");
      when(driverInfo.version()).thenReturn(Optional.of("4.15.0"));
      when(clientInfo.driverInfo()).thenReturn(Optional.of(driverInfo));
      Tags result = ClientInfoMetricsTagProvider.DEFAULT.getClientInfoTags(clientInfo);
      assertThat(result)
          .contains(Tag.of("driverName", "driver1"), Tag.of("driverVersion", "4.15.0"));
    }

    @Test
    public void missingDriverVersion() {
      ClientInfo clientInfo = mock(ClientInfo.class);
      DriverInfo driverInfo = mock(DriverInfo.class);
      when(driverInfo.name()).thenReturn("driver1");
      when(driverInfo.version()).thenReturn(Optional.ofNullable(null));
      when(clientInfo.driverInfo()).thenReturn(Optional.of(driverInfo));
      Tags result = ClientInfoMetricsTagProvider.DEFAULT.getClientInfoTags(clientInfo);
      assertThat(result)
          .contains(Tag.of("driverName", "driver1"), Tag.of("driverVersion", "unknown"));
    }

    @Test
    public void missingDriverInfo() {
      ClientInfo clientInfo = mock(ClientInfo.class);
      when(clientInfo.driverInfo()).thenReturn(Optional.ofNullable(null));
      Tags result = ClientInfoMetricsTagProvider.DEFAULT.getClientInfoTags(clientInfo);
      assertThat(result)
          .contains(Tag.of("driverName", "unknown"), Tag.of("driverVersion", "unknown"));
    }
  }
}
