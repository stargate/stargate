package io.stargate.db.query;

public enum QueryType {
  SELECT,
  INSERT,
  UPDATE,
  DELETE,
  BATCH,
  OTHER;

  /** Whether this is the kind of a DML query (whether it modifies data). */
  public boolean isDML() {
    switch (this) {
      case INSERT:
      case UPDATE:
      case DELETE:
      case BATCH:
        return true;
      default:
        return false;
    }
  }
}
