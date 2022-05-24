package io.stargate.sgv2.common.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ApiTimingDiagnosticsTest {
  @Test
  public void testSamplerDefParsingForInterval() {
    assertThat(ApiTimingDiagnosticsSampler.fromString("5s").toString())
        .isEqualTo("for event at most every 5 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12s").toString())
        .isEqualTo("for event at most every 12 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12.0s").toString())
        .isEqualTo("for event at most every 12 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12.75s").toString())
        .isEqualTo("for event at most every 12.75 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("12.750s").toString())
        .isEqualTo("for event at most every 12.75 seconds");
    assertThat(ApiTimingDiagnosticsSampler.fromString("0.5s").toString())
        .isEqualTo("for event at most every 0.5 seconds");
  }

  @Test
  public void testSamplerDefParsingForProbability() {
    assertThat(ApiTimingDiagnosticsSampler.fromString("50%").toString()).isEqualTo("for 50% of events");
    assertThat(ApiTimingDiagnosticsSampler.fromString("15%").toString()).isEqualTo("for 15% of events");
    assertThat(ApiTimingDiagnosticsSampler.fromString("15.00%").toString()).isEqualTo("for 15% of events");
    assertThat(ApiTimingDiagnosticsSampler.fromString("0.25%").toString()).isEqualTo("for 0.25% of events");
  }

  @Test
  public void testSamplerDefParsingForNone() {
    assertThat(ApiTimingDiagnosticsSampler.fromString("").toString()).isEqualTo("for none of events");
    assertThat(ApiTimingDiagnosticsSampler.fromString("none").toString()).isEqualTo("for none of events");
  }
}
