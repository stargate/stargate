package io.stargate.db.query;

import java.util.List;

public interface BoundInsert extends BoundDMLQuery {
  /**
   * Like updates, inserts cannot impact a range of rows, but contrarily to updates, inserts can
   * only impact a single row. This method is just a shortcut to returns that primary key.
   */
  default PrimaryKey primaryKey() {
    assert !rowsUpdated().isRanges();
    List<PrimaryKey> keys = rowsUpdated().asKeys().primaryKeys();
    assert keys.size() == 1;
    return keys.get(0);
  }

  boolean ifNotExists();
}
