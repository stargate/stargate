package io.stargate.sgv2.it;

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
