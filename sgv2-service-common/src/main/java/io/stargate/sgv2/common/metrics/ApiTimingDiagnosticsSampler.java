package io.stargate.sgv2.common.metrics;

import java.math.BigDecimal;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class that implements logic for sampling a subset of possible diagnostics entries to log,
 * used with {@link ApiTimingDiagnosticsFactory} and {@link ApiTimingDiagnostics}.
 *
 * <p>Two real modes are supported:
 *
 * <ul>
 *   <li>Sample certain percentage of all entries, like {@code "25%"} (probability)
 *   <li>Sample an entry with at least specified time interval between samples, likke {@code "5s"}
 *       (emit sample at most every 5 seconds)
 * </ul>
 *
 * along with psuedo-mode of {@code "none"} (which samples nothing as name implies).
 */
public abstract class ApiTimingDiagnosticsSampler {
  private static final Pattern INPUT_PATTERN =
      Pattern.compile("([\\d]+(\\.[\\d]+)?)\\s*(s|sec|secs|%)");

  public abstract boolean include();

  /** @param pct Percentage change of using a sample, [0.0, 100.0] */
  public static ApiTimingDiagnosticsSampler probabilitySampler(BigDecimal pct) {
    return new ProbabilityBased(pct);
  }

  /** @param intervalSecs Minimum amount of time between sampled entries, in seconds */
  public static ApiTimingDiagnosticsSampler intervalSampler(BigDecimal intervalSecs) {
    return new IntervalBased(intervalSecs);
  }

  public static ApiTimingDiagnosticsSampler noneSampler() {
    return new None();
  }

  public static ApiTimingDiagnosticsSampler fromString(String input)
      throws IllegalArgumentException {
    if ((input == null) || (input = input.trim()).isEmpty() || "none".equals(input)) {
      return noneSampler();
    }

    Matcher m = INPUT_PATTERN.matcher(input.trim());

    if (m.matches()) {
      try {
        // Parse using BigDecimal to retain info on which fractions (if any) provided
        BigDecimal value = new BigDecimal(m.group(1));
        if (m.group(3).equals("%")) {
          // given as percents
          return probabilitySampler(value);
        }
        // given as seconds
        return intervalSampler(value);

      } catch (NumberFormatException e) {
      }
    }
    throw new IllegalArgumentException(
        "argument '"
            + input
            + "' invalid: should have value like '10s' (at most every 10 seconds), '5%' (5 percent) or 'none' (none)");
  }

  /*
  ///////////////////////////////////////////////////////////////////////
  // Standard implementations
  ///////////////////////////////////////////////////////////////////////
   */

  private static class None extends ApiTimingDiagnosticsSampler {
    @Override
    public boolean include() {
      return false;
    }

    @Override
    public String toString() {
      return "none";
    }
  }

  /** Simple probability-based filter for sampling certain percentage of entries. */
  private static class ProbabilityBased extends ApiTimingDiagnosticsSampler {
    private final Random rnd;
    private final double probability;
    private final String desc;

    public ProbabilityBased(BigDecimal pct) {
      this.probability = pct.divide(BigDecimal.valueOf(100L)).doubleValue();
      // choice does not matter a lot but keep it reproducible
      rnd = new Random(Double.hashCode(probability));
      desc = String.format("%s%%", pct.stripTrailingZeros().toPlainString());
    }

    @Override
    public boolean include() {
      // java.util.Random instances are thread-safe so no sync needed
      return rnd.nextDouble() < probability;
    }

    @Override
    public String toString() {
      return desc;
    }
  }

  /** Filter that implements "at most every N seconds" styling sampling. */
  private static class IntervalBased extends ApiTimingDiagnosticsSampler {
    private final long msecsBetween;
    private final String desc;

    private final AtomicLong noSamplesUntil;

    public IntervalBased(BigDecimal secs) {
      this.msecsBetween = secs.multiply(BigDecimal.valueOf(1000L)).longValue();
      noSamplesUntil = new AtomicLong(System.currentTimeMillis() + msecsBetween);
      desc = String.format("at most every %s seconds", secs.stripTrailingZeros().toPlainString());
    }

    // Since we have state, need to synchronize. Should not be called often enough
    // for sync performance to greatly matter so use a simple enough approach
    @Override
    public synchronized boolean include() {
      final long now = System.currentTimeMillis();
      final long barrier = noSamplesUntil.get();

      // Most likely case (esp. if heavy load): not yet allowed
      if (now < barrier) {
        return false;
      }
      // But now verify the optimistic locking by trying to set
      return noSamplesUntil.compareAndSet(barrier, now + msecsBetween);
    }

    @Override
    public String toString() {
      return desc;
    }
  }
}
