package io.stargate.sgv2.docsapi.api.common.properties.document;

import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/** Helper for understanding the available document table columns. */
public interface DocumentTableColumns {

  /** @return All columns as the {@link ImmutableColumn} representation. */
  List<Column> allColumns();

  /** @return All names of columns in an array, ordered in same way as {@link #allColumns()} */
  String[] allColumnNamesArray();

  /** @return Value columns, including the leaf, as {@link Set}. */
  Set<String> valueColumnNames();

  /** @return All the JSON path columns based on the max depth as {@link Set}. */
  Set<String> pathColumnNames();

  /** @return All the JSON path columns based on the max depth as ordered {@link List}. */
  List<String> pathColumnNamesList();

  /**
   * Provides a stream of all columns names, but with the path columns being limited to given depth.
   *
   * @param depth max depth of the path columns
   * @return Stream
   */
  default Stream<String> allColumnNamesWithPathDepth(int depth) {
    List<String> path = pathColumnNamesList().subList(0, depth);

    return allColumns().stream()
        .map(Column::name)
        .filter(c -> path.contains(c) || !pathColumnNamesList().contains(c));
  }
}
