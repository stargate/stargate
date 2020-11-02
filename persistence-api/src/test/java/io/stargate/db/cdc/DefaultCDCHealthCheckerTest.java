package io.stargate.db.cdc;

import static io.stargate.db.cdc.DefaultCDCHealthChecker.INTERVAL;
import static io.stargate.db.cdc.DefaultCDCHealthChecker.TICK_INTERVAL;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class DefaultCDCHealthCheckerTest {
  @ParameterizedTest
  @MethodSource("constructorWithInvalidParameters")
  public void shouldThrowWithInvalidConstructorParameters(Executable executable) {
    assertThrows(IllegalArgumentException.class, executable);
  }

  @ParameterizedTest
  @MethodSource("constructorWithValidParameters")
  public void shouldNotThrowWithValidConstructorParameters(Executable executable) {
    assertDoesNotThrow(executable);
  }

  public static Stream<Executable> constructorWithInvalidParameters() {
    return Stream.of(
        () -> new DefaultCDCHealthChecker(0, 1, 1),
        () -> new DefaultCDCHealthChecker(1.1, 1, 1),
        () -> new DefaultCDCHealthChecker(0.5, 1, 0),
        () -> new DefaultCDCHealthChecker(0.5, 1, 16));
  }

  public static Stream<Executable> constructorWithValidParameters() {
    return Stream.of(
        () -> new DefaultCDCHealthChecker(0.5, 1, 1),
        () -> new DefaultCDCHealthChecker(0.5, 1, 15));
  }

  @Test
  public void shouldBeHealthyInitially() {
    AtomicLong ticks = new AtomicLong();
    DefaultCDCHealthChecker checker = new DefaultCDCHealthChecker(0.5, 1, 15, ticks::get);
    assertThat(checker.isHealthy()).isTrue();
  }

  @Test
  public void shouldConsiderMinErrorsPerSecond() {
    final int ewmaMinutes = 1;
    final int minErrorsPerSecond = 1;
    AtomicLong ticks = new AtomicLong();
    DefaultCDCHealthChecker checker =
        new DefaultCDCHealthChecker(0.01, minErrorsPerSecond, ewmaMinutes, ticks::get);

    checker.reportSendError();
    checker.reportSendError();
    setTimePassed(ticks);
    assertThat(checker.isHealthy()).isTrue();

    long occurrences = MINUTES.toSeconds(minErrorsPerSecond);
    for (int i = 0; i < occurrences; i++) {
      checker.reportSendError();
    }
    setTimePassed(ticks);
    assertThat(checker.isHealthy()).isFalse();
  }

  @Test
  public void shouldConsiderErrorRateThreshold() {
    final double errorRateThreshold = 0.501;
    final int minErrorsPerSecond = 2;
    AtomicLong ticks = new AtomicLong();
    DefaultCDCHealthChecker checker =
        new DefaultCDCHealthChecker(errorRateThreshold, minErrorsPerSecond, 1, ticks::get);
    long occurrences = MINUTES.toSeconds(minErrorsPerSecond) / INTERVAL;

    // 50% error and success
    for (int i = 0; i < occurrences; i++) {
      checker.reportSendSuccess();
      checker.reportSendError();
    }

    setTimePassed(ticks);
    // It should be healthy because it doesn't reached the threshold
    assertThat(checker.isHealthy()).isTrue();

    // More errors to pass the threshold
    for (int i = 0; i < occurrences; i++) {
      checker.reportSendError();
    }

    setTimePassed(ticks);
    // It should be unhealthy because the amount of errors is greater than errorRateThreshold
    assertThat(checker.isHealthy()).isFalse();
  }

  @Test
  public void shouldConsiderBeBackToHealthyOnceTheTimePasses() {
    final double errorRateThreshold = 0.4;
    final int minErrorsPerSecond = 1;
    AtomicLong ticks = new AtomicLong();
    DefaultCDCHealthChecker checker =
        new DefaultCDCHealthChecker(errorRateThreshold, minErrorsPerSecond, 1, ticks::get);
    long occurrences = MINUTES.toSeconds(minErrorsPerSecond) / INTERVAL;

    // 50% error and success
    for (int i = 0; i < occurrences; i++) {
      checker.reportSendSuccess();
      checker.reportSendError();
    }
    setTimePassed(ticks);
    setTimePassed(ticks);
    // Marked unhealthy because it passed the threshold
    assertThat(checker.isHealthy()).isFalse();

    // 50s
    setTimePassed(ticks, 10);
    // It should be healthy because minErrorsPerSecond is not reached
    assertThat(checker.isHealthy()).isTrue();
  }

  /** Fakes the clock moving forward n times */
  private static void setTimePassed(AtomicLong ticks, int numTickIntervals) {
    for (int i = 0; i < numTickIntervals; i++) {
      ticks.addAndGet(TICK_INTERVAL + 1);
    }
  }

  private static void setTimePassed(AtomicLong ticks) {
    setTimePassed(ticks, 1);
  }
}
