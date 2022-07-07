package io.stargate.db.query;

public enum Predicate {
  EQ("="),
  NEQ("!="),
  LT("<"),
  GT(">"),
  LTE("<="),
  GTE(">="),
  IN("IN"),
  CONTAINS("CONTAINS"),
  CONTAINS_KEY("CONTAINS KEY");

  private final String cql;

  Predicate(String cql) {
    this.cql = cql;
  }

  @Override
  public String toString() {
    return cql;
  }
}
