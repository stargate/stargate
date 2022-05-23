package io.stargate.sgv2.common.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ApiTimingDiagnosticsTest {
  @Test
  public void testSamplerDefParsingForInterval() {
    assertThat(ApiTimingDiagnosticsSampler.fromString("5s").toString())
        .isEqualTo("at most every 5 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12s").toString())
        .isEqualTo("at most every 12 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12.0s").toString())
        .isEqualTo("at most every 12 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12.75s").toString())
        .isEqualTo("at most every 12.75 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12.750s").toString())
        .isEqualTo("at most every 12.75 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("0.5s").toString())
        .isEqualTo("at most every 0.5 seconds");
  }

  @Test
  public void testSamplerDefParsingForProbability() {
    assertThat(ApiTimingDiagnosticsSampler.fromString("50%").toString()).isEqualTo("50%");
    assertThat(ApiTimingDiagnosticsSampler.fromString("15%").toString()).isEqualTo("15%");
    assertThat(ApiTimingDiagnosticsSampler.fromString("15.00%").toString()).isEqualTo("15%");
    assertThat(ApiTimingDiagnosticsSampler.fromString("0.25%").toString()).isEqualTo("0.25%");
  }

  @Test
  public void testSamplerDefParsingForNone() {
    assertThat(ApiTimingDiagnosticsSampler.fromString("").toString()).isEqualTo("none");
    assertThat(ApiTimingDiagnosticsSampler.fromString("none").toString()).isEqualTo("none");
  }
}
