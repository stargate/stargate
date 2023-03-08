package io.stargate.it;

/**
 * Simple constants for the integration test order, due to the fact that the
 * org.junit.jupiter.api.{@link org.junit.jupiter.api.ClassOrderer.OrderAnnotation} is active by
 * default in this suite.
 */
public final class TestOrder {

  // basic
  public static final int FIRST = Integer.MIN_VALUE;
  public static final int LAST = Integer.MAX_VALUE;
}
