package io.stargate.sgv2.common.metrics;

import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ApiTimingDiagnostics {
  private final String operation;
  private final long startTimeNanos;
  private long endTimeNanos;

  private final Timer tableSchemaTimer;
  private int tableSchemaCount;
  private long tableSchemaNanos;

  private final Timer dbReadTimer;
  private int dbReadCount;
  private long dbReadNanos;

  private final Timer dbWriteTimer;
  private int dbWriteCount;
  private long dbWriteNanos;

  public ApiTimingDiagnostics(
      String operation, Timer tableSchemaTimer, Timer dbReadTimer, Timer dbWriteTimer) {
    this.operation = operation;
    this.tableSchemaTimer = tableSchemaTimer;
    this.dbReadTimer = dbReadTimer;
    this.dbWriteTimer = dbWriteTimer;

    startTimeNanos = System.nanoTime();
  }

  public <T> T timedTableSchemaAccess(Supplier<T> toCall) {
    final long startTimeNanos = currentNanos();
    try {
      return toCall.get();
    } finally {
      tableSchemaNanos += updateTimer(tableSchemaTimer, startTimeNanos);
      ++tableSchemaCount;
    }
  }

  public <T> T timedDbRead(Supplier<T> toCall) {
    final long startTimeNanos = currentNanos();
    try {
      return toCall.get();
    } finally {
      dbReadNanos += updateTimer(dbReadTimer, startTimeNanos);
      ++dbReadCount;
    }
  }

  public <T> T timedDbWrite(Supplier<T> toCall) {
    final long startTimeNanos = currentNanos();
    try {
      return toCall.get();
    } finally {
      dbWriteNanos += updateTimer(dbWriteTimer, startTimeNanos);
      ++dbWriteCount;
    }
  }

  public ApiTimingDiagnostics markEndTime() {
    endTimeNanos = System.nanoTime();
    return this;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(100);
    sb.append("operation=").append(operation);
    final long totalNanos = totalTimeNanos();
    sb.append(" total-time=").append(nanosToMsecString(totalNanos));
    long otherNanos = totalNanos;
    if (tableSchemaCount > 0) {
      sb.append(",table-schema-access(")
          .append(tableSchemaCount)
          .append(")=")
          .append(nanosToMsecString(tableSchemaNanos));
      otherNanos -= tableSchemaNanos;
    }
    if (dbReadCount > 0) {
      sb.append(",db-read(")
          .append(dbReadCount)
          .append(")=")
          .append(nanosToMsecString(dbReadNanos));
      otherNanos -= dbReadNanos;
    }
    if (dbWriteCount > 0) {
      sb.append(",db-write(")
          .append(dbWriteCount)
          .append(")=")
          .append(nanosToMsecString(dbWriteNanos));
      otherNanos -= dbWriteNanos;
    }
    sb.append(",other=").append(nanosToMsecString(otherNanos));
    return sb.toString();
  }

  private long currentNanos() {
    return System.nanoTime();
  }

  private long totalTimeNanos() {
    long lapsed = endTimeNanos - startTimeNanos;
    return Math.max(lapsed, 0L);
  }

  private long updateTimer(Timer timer, long startTimeNanos) {
    final long nanosElapsed = currentNanos() - startTimeNanos;
    timer.record(nanosElapsed, TimeUnit.NANOSECONDS);
    return nanosElapsed;
  }

  static String nanosToMsecString(long nanos) {
    // Ok so FP formatting is truly slow (see f.ex
    // https://stackoverflow.com/questions/10553710/fast-double-to-string-conversion-with-given-precision)
    // and we start with integral number so could consider optimizing.
    // But start with simple code for now; we are not to print out formatted numbers in production
    // (except with low sampling rate, if that)

    double msecs = nanos / 1_000_000.0;
    return String.format("%.2f msec", msecs);
  }
}
