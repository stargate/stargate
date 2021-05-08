package io.stargate.web.docsapi.service;

public interface TimeSource {

  TimeSource SYSTEM = () -> System.currentTimeMillis() * 1000;

  /** Microseconds since {@link java.lang.System#currentTimeMillis() epoch}. */
  long currentTimeMicros();
}
