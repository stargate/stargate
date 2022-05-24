package io.stargate.sgv2.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.function.Function;
import org.slf4j.Logger;

public class ApiTimingDiagnosticsFactory {
  private final Logger logger;
  private final ApiTimingDiagnosticsSampler sampler;
  private final Timer tableSchemaTimer;
  private final Timer dbReadTimer, dbWriteTimer;

  private ApiTimingDiagnosticsFactory(
      Logger logger,
      ApiTimingDiagnosticsSampler sampler,
      Timer tableSchemaTimer,
      Timer dbReadTimer,
      Timer dbWriteTimer) {
    this.logger = logger;
    this.sampler = sampler;
    this.tableSchemaTimer = tableSchemaTimer;
    this.dbReadTimer = dbReadTimer;
    this.dbWriteTimer = dbWriteTimer;
  }

  // Note: we must pass "MeterRegistry" to export metrics to Prometheus; this does
  // not use the global default registry
  public static ApiTimingDiagnosticsFactory createFactory(
      MeterRegistry mmMeters, String prefix, Logger logger, ApiTimingDiagnosticsSampler sampler) {
    return new ApiTimingDiagnosticsFactory(
        logger,
        sampler,
        mmMeters.timer(prefix + "bridge-table-access"),
        mmMeters.timer(prefix + "bridge-db-read"),
        mmMeters.timer(prefix + "bridge-db-write"));
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
      if (sampler.include()) {
        logger.info("TIMING: {}", diagnostics);
      }
    }
  }
}
