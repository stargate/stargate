package io.stargate.sgv2.docsapi.api.common.properties.document;

import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import java.util.List;
import java.util.Set;

/** Helper for understanding the available document table columns. */
public interface DocumentTableColumns {

  /** @return All columns as the {@link ImmutableColumn} representation. */
  List<Column> allColumns();

  /** @return Value columns, including the leaf, as {@link Set}. */
  Set<String> valueColumnNames();

  /** @return All the JSON path columns based on the max depth as {@link Set}. */
  Set<String> pathColumnNames();

  /** @return All the JSON path columns based on the max depth as ordered {@link List}. */
  List<String> pathColumnNamesList();

  // TODO document, optimize
  default List<String> allColumnNamesWithPathDepth(int depth) {
    List<String> path = pathColumnNamesList().subList(0, depth);

    return allColumns().stream()
        .map(Column::name)
        .filter(c -> path.contains(c) || !pathColumnNamesList().contains(c))
        .toList();
  }
}
