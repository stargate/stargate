package io.stargate.bridge.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.UnsupportedTemporalTypeException;
import java.util.Locale;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CqlDurationTest {

  @ParameterizedTest
  @MethodSource("locales")
  public void should_parse_from_string_with_standard_pattern(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      Assertions.assertThat(CqlDuration.from("1y2mo")).isEqualTo(CqlDuration.newInstance(14, 0, 0));
      assertThat(CqlDuration.from("-1y2mo")).isEqualTo(CqlDuration.newInstance(-14, 0, 0));
      assertThat(CqlDuration.from("1Y2MO")).isEqualTo(CqlDuration.newInstance(14, 0, 0));
      assertThat(CqlDuration.from("2w")).isEqualTo(CqlDuration.newInstance(0, 14, 0));
      assertThat(CqlDuration.from("2d10h"))
          .isEqualTo(CqlDuration.newInstance(0, 2, 10 * CqlDuration.NANOS_PER_HOUR));
      assertThat(CqlDuration.from("2d")).isEqualTo(CqlDuration.newInstance(0, 2, 0));
      assertThat(CqlDuration.from("30h"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 30 * CqlDuration.NANOS_PER_HOUR));
      assertThat(CqlDuration.from("30h20m"))
          .isEqualTo(
              CqlDuration.newInstance(
                  0, 0, 30 * CqlDuration.NANOS_PER_HOUR + 20 * CqlDuration.NANOS_PER_MINUTE));
      assertThat(CqlDuration.from("20m"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 20 * CqlDuration.NANOS_PER_MINUTE));
      assertThat(CqlDuration.from("56s"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 56 * CqlDuration.NANOS_PER_SECOND));
      assertThat(CqlDuration.from("567ms"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 567 * CqlDuration.NANOS_PER_MILLI));
      assertThat(CqlDuration.from("1950us"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 1950 * CqlDuration.NANOS_PER_MICRO));
      assertThat(CqlDuration.from("1950µs"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 1950 * CqlDuration.NANOS_PER_MICRO));
      assertThat(CqlDuration.from("1950000ns")).isEqualTo(CqlDuration.newInstance(0, 0, 1950000));
      assertThat(CqlDuration.from("1950000NS")).isEqualTo(CqlDuration.newInstance(0, 0, 1950000));
      assertThat(CqlDuration.from("-1950000ns")).isEqualTo(CqlDuration.newInstance(0, 0, -1950000));
      assertThat(CqlDuration.from("1y3mo2h10m"))
          .isEqualTo(CqlDuration.newInstance(15, 0, 130 * CqlDuration.NANOS_PER_MINUTE));
    } finally {
      Locale.setDefault(def);
    }
  }

  @ParameterizedTest
  @MethodSource("locales")
  public void should_parse_from_string_with_iso8601_pattern(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(CqlDuration.from("P1Y2D")).isEqualTo(CqlDuration.newInstance(12, 2, 0));
      assertThat(CqlDuration.from("P1Y2M")).isEqualTo(CqlDuration.newInstance(14, 0, 0));
      assertThat(CqlDuration.from("P2W")).isEqualTo(CqlDuration.newInstance(0, 14, 0));
      assertThat(CqlDuration.from("P1YT2H"))
          .isEqualTo(CqlDuration.newInstance(12, 0, 2 * CqlDuration.NANOS_PER_HOUR));
      assertThat(CqlDuration.from("-P1Y2M")).isEqualTo(CqlDuration.newInstance(-14, 0, 0));
      assertThat(CqlDuration.from("P2D")).isEqualTo(CqlDuration.newInstance(0, 2, 0));
      assertThat(CqlDuration.from("PT30H"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 30 * CqlDuration.NANOS_PER_HOUR));
      assertThat(CqlDuration.from("PT30H20M"))
          .isEqualTo(
              CqlDuration.newInstance(
                  0, 0, 30 * CqlDuration.NANOS_PER_HOUR + 20 * CqlDuration.NANOS_PER_MINUTE));
      assertThat(CqlDuration.from("PT20M"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 20 * CqlDuration.NANOS_PER_MINUTE));
      assertThat(CqlDuration.from("PT56S"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 56 * CqlDuration.NANOS_PER_SECOND));
      assertThat(CqlDuration.from("P1Y3MT2H10M"))
          .isEqualTo(CqlDuration.newInstance(15, 0, 130 * CqlDuration.NANOS_PER_MINUTE));
    } finally {
      Locale.setDefault(def);
    }
  }

  @ParameterizedTest
  @MethodSource("locales")
  public void should_parse_from_string_with_iso8601_alternative_pattern(Locale locale) {
    Locale def = Locale.getDefault();
    try {
      Locale.setDefault(locale);
      assertThat(CqlDuration.from("P0001-00-02T00:00:00"))
          .isEqualTo(CqlDuration.newInstance(12, 2, 0));
      assertThat(CqlDuration.from("P0001-02-00T00:00:00"))
          .isEqualTo(CqlDuration.newInstance(14, 0, 0));
      assertThat(CqlDuration.from("P0001-00-00T02:00:00"))
          .isEqualTo(CqlDuration.newInstance(12, 0, 2 * CqlDuration.NANOS_PER_HOUR));
      assertThat(CqlDuration.from("-P0001-02-00T00:00:00"))
          .isEqualTo(CqlDuration.newInstance(-14, 0, 0));
      assertThat(CqlDuration.from("P0000-00-02T00:00:00"))
          .isEqualTo(CqlDuration.newInstance(0, 2, 0));
      assertThat(CqlDuration.from("P0000-00-00T30:00:00"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 30 * CqlDuration.NANOS_PER_HOUR));
      assertThat(CqlDuration.from("P0000-00-00T30:20:00"))
          .isEqualTo(
              CqlDuration.newInstance(
                  0, 0, 30 * CqlDuration.NANOS_PER_HOUR + 20 * CqlDuration.NANOS_PER_MINUTE));
      assertThat(CqlDuration.from("P0000-00-00T00:20:00"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 20 * CqlDuration.NANOS_PER_MINUTE));
      assertThat(CqlDuration.from("P0000-00-00T00:00:56"))
          .isEqualTo(CqlDuration.newInstance(0, 0, 56 * CqlDuration.NANOS_PER_SECOND));
      assertThat(CqlDuration.from("P0001-03-00T02:10:00"))
          .isEqualTo(CqlDuration.newInstance(15, 0, 130 * CqlDuration.NANOS_PER_MINUTE));
    } finally {
      Locale.setDefault(def);
    }
  }

  @Test
  public void should_fail_to_parse_invalid_durations() {
    assertInvalidDuration(
        Long.MAX_VALUE + "d",
        "Invalid duration. The total number of days must be less or equal to 2147483647");
    assertInvalidDuration("2µ", "Unable to convert '2µ' to a duration");
    assertInvalidDuration("-2µ", "Unable to convert '2µ' to a duration");
    assertInvalidDuration("12.5s", "Unable to convert '12.5s' to a duration");
    assertInvalidDuration("2m12.5s", "Unable to convert '2m12.5s' to a duration");
    assertInvalidDuration("2m-12s", "Unable to convert '2m-12s' to a duration");
    assertInvalidDuration("12s3s", "Invalid duration. The seconds are specified multiple times");
    assertInvalidDuration("12s3m", "Invalid duration. The seconds should be after minutes");
    assertInvalidDuration("1Y3M4D", "Invalid duration. The minutes should be after days");
    assertInvalidDuration("P2Y3W", "Unable to convert 'P2Y3W' to a duration");
    assertInvalidDuration("P0002-00-20", "Unable to convert 'P0002-00-20' to a duration");
  }

  private void assertInvalidDuration(String duration, String expectedErrorMessage) {
    try {
      CqlDuration.from(duration);
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertThat(e.getMessage()).isEqualTo(expectedErrorMessage);
    }
  }

  @Test
  public void should_get_by_unit() {
    CqlDuration duration = CqlDuration.from("3mo2d15s");
    assertThat(duration.get(ChronoUnit.MONTHS)).isEqualTo(3);
    assertThat(duration.get(ChronoUnit.DAYS)).isEqualTo(2);
    assertThat(duration.get(ChronoUnit.NANOS)).isEqualTo(15 * CqlDuration.NANOS_PER_SECOND);
    assertThatThrownBy(() -> duration.get(ChronoUnit.YEARS))
        .isInstanceOf(UnsupportedTemporalTypeException.class);
  }

  @Test
  public void should_add_to_temporal() {
    ZonedDateTime dateTime = ZonedDateTime.parse("2018-10-04T00:00-07:00[America/Los_Angeles]");
    assertThat(dateTime.plus(CqlDuration.from("1mo")))
        .isEqualTo("2018-11-04T00:00-07:00[America/Los_Angeles]");
    assertThat(dateTime.plus(CqlDuration.from("1mo1h10s")))
        .isEqualTo("2018-11-04T01:00:10-07:00[America/Los_Angeles]");
    // 11-04 2:00 is daylight saving time end
    assertThat(dateTime.plus(CqlDuration.from("1mo3h")))
        .isEqualTo("2018-11-04T02:00-08:00[America/Los_Angeles]");
  }

  @Test
  public void should_subtract_from_temporal() {
    ZonedDateTime dateTime = ZonedDateTime.parse("2018-10-04T00:00-07:00[America/Los_Angeles]");
    assertThat(dateTime.minus(CqlDuration.from("2mo")))
        .isEqualTo("2018-08-04T00:00-07:00[America/Los_Angeles]");
    assertThat(dateTime.minus(CqlDuration.from("1h15s15ns")))
        .isEqualTo("2018-10-03T22:59:44.999999985-07:00[America/Los_Angeles]");
  }

  public static Arguments[] locales() {
    return new Arguments[] {
      arguments(Locale.US),
      // non-latin alphabets
      arguments(Locale.CHINA),
      arguments(Locale.JAPAN),
      arguments(Locale.KOREA),
      arguments(new Locale("gr") /* greek */),
      arguments(new Locale("ar") /* arabic */),
      // latin-based alphabets with extended character sets
      arguments(new Locale("vi") /* vietnamese */),
      arguments(new Locale("tr") /* turkish*/),
    };
  }
}
