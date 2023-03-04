package io.stargate.db.query;

import java.util.List;

public interface BoundUpdate extends BoundDMLQueryWithConditions {
  /**
   * As range updates are not supported by CQL, an UPDATE {@link #rowsUpdated()} is necessarily a
   * list of primary keys (only 1 in most case, but potentially more than one with a IN). This
   * method just returns those primary keys.
   */
  default List<PrimaryKey> primaryKeys() {
    assert !rowsUpdated().isRanges();
    return rowsUpdated().asKeys().primaryKeys();
  }
}
