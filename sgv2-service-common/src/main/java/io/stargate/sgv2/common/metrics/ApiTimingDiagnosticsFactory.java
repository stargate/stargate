package io.stargate.sgv2.common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.function.Function;

public class ApiTimingDiagnosticsFactory {
  private final Timer tableSchemaTimer;

  private ApiTimingDiagnosticsFactory(Timer tableSchemaTimer) {
    this.tableSchemaTimer = tableSchemaTimer;
  }

  public static ApiTimingDiagnosticsFactory createFactory(
      MetricRegistry metricsRegistry, String prefix) {
    return new ApiTimingDiagnosticsFactory(metricsRegistry.timer(prefix + "bridge-table-access"));
  }

  public ApiTimingDiagnostics createDiagnostics(String operation) {
    return new ApiTimingDiagnostics(operation, tableSchemaTimer);
  }

  public <T> T callWithDiagnostics(String operation, Function<ApiTimingDiagnostics, T> function) {
    final ApiTimingDiagnostics diagnostics = createDiagnostics(operation);
    try {
      return function.apply(diagnostics);
    } finally {
      diagnostics.markEndTime();
      // !!! TODO: configure with sampling frequency or minimum time between samples;
      //   only printing all entries during development
      System.err.println("TIMING: " + diagnostics);
    }
  }
}
