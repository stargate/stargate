package io.stargate.db.query;

import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A fully bound SELECT query.
 *
 * <p>Please note that the method of this class currently only expose a <b>very</b> restricted
 * subset of what a SELECT query may contain. This is meant to evolve over time so more details are
 * exposed.
 */
public interface BoundSelect extends BoundQuery {

  AbstractTable table();

  /**
   * The subset of the {@link #table} columns that are queried by this SELECT, or an empty set for
   * "star" SELECT.
   *
   * <p>Please note that this does not necessarily directly map to the entries in the result set of
   * this SELECT, as the result set may have entries that are not just a table column. For instance,
   * for {@code SELECT x, SUM(y + x), TTL(z) FROM ...}, this method will return a set containing
   * columns x, y and z (but the result sets are more "composite" entries).
   *
   * @return a set with the columns of {@link #table} that this SELECT has to query, or an empty set
   *     if this is a "star" select ({@code SELECT * FROM ...}).
   */
  Set<Column> selectedColumns();

  default boolean isStarSelect() {
    return selectedColumns().isEmpty();
  }

  /**
   * If the SELECT selects a set of rows "by name", or if it selects some slices of rows within a
   * single partition key, the description of the row selected. If the select is neither of those
   * case (typically, it select a range of partitions), then the optional will be empty.
   */
  Optional<RowsImpacted> selectedRows();

  BoundSelect withAddedSelectedColumns(Set<Column> columns);

  OptionalInt limit();
}
