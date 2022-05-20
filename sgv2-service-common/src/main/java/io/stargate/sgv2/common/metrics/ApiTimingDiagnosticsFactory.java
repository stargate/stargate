package io.stargate.sgv2.common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.function.Function;

public class ApiTimingDiagnosticsFactory {
  private final Timer tableSchemaTimer;
  private final Timer dbReadTimer, dbWriteTimer;

  private ApiTimingDiagnosticsFactory(
      Timer tableSchemaTimer, Timer dbReadTimer, Timer dbWriteTimer) {
    this.tableSchemaTimer = tableSchemaTimer;
    this.dbReadTimer = dbReadTimer;
    this.dbWriteTimer = dbWriteTimer;
  }

  public static ApiTimingDiagnosticsFactory createFactory(
      MetricRegistry metricsRegistry, String prefix) {
    return new ApiTimingDiagnosticsFactory(
        metricsRegistry.timer(prefix + "bridge-table-access"),
        metricsRegistry.timer(prefix + "bridge-db-read"),
        metricsRegistry.timer(prefix + "bridge-db-write"));
  }

  public ApiTimingDiagnostics createDiagnostics(String operation) {
    return new ApiTimingDiagnostics(operation, tableSchemaTimer, dbReadTimer, dbWriteTimer);
  }

  public <T> T withDiagnostics(String operation, Function<ApiTimingDiagnostics, T> function) {
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
