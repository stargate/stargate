package io.stargate.sgv2.it;

/**
 * Interface for testing the REST API v2 with a map column. There are a couple of implementations
 * for this interface 1. An implementation with a compact map column as the default {@link
 * RestApiV2QMapCompactIT} 2. An implementation with a non-compact map column as the default {@link
 * RestApiV2QMapCompactDisabledIT}
 *
 * <p>The same set of tests should be run for both scenarios. This interface is a contract for the
 * tests to be run.
 */
public interface RestApiV2QMapTests {
  public void addRowWithCompactMap();

  public void addRowWithNonCompactMap();

  public void updateRowWithCompactMap();

  public void updateRowWithNonCompactMap();

  public void patchRowWithCompactMap();

  public void patchRowWithNonCompactMap();

  public void getRowsWithCompactMap();

  public void getRowsWithNonCompactMap();

  public void getAllRowsWithCompactMap();

  public void getAllRowsWithNonCompactMap();

  public void getRowsWithWhereWithCompactMap();

  public void getRowsWithWhereWithNonCompactMap();

  public void getAllIndexesWithCompactMap();

  public void getAllIndexesWithNonCompactMap();
}
