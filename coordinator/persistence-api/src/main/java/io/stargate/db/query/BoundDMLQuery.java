package io.stargate.db.query;

import io.stargate.db.schema.Table;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

public interface BoundDMLQuery extends BoundQuery {
  Table table();

  /**
   * Identifies the row(s) affected by this DML query.
   *
   * @return the rows updated by this query.
   */
  RowsImpacted rowsUpdated();

  /**
   * The modification to non-primary key columns made by this DML query.
   *
   * @return the list of modification made by this query. This will never be empty for an UPDATE,
   *     but can be for an INSERT that only inserts the primary key column. For DELETE, an empty
   *     list signify a full row deletion (or multiple ones if the DELETE is a range or partition
   *     deletion), while a non-empty list signify that only some subset of columns are deleted. Do
   *     note that deletion of a specific column is signified by a modification whose {@link
   *     Modification#operation()} is {@link Modification.Operation#SET} and whose {@link
   *     Modification#value()} is {@code null} (and this is the only type of modification that a
   *     DELETE can have).
   */
  List<Modification> modifications();

  OptionalInt ttl();

  OptionalLong timestamp();
}
