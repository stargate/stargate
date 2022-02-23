package io.stargate.bridge.service.docsapi;

import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltSelect;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** A helper class for dealing with a list of related queries. */
@org.immutables.value.Value.Immutable(lazyhash = true)
public abstract class QueryData {

  abstract List<BoundQuery> queries();

  @org.immutables.value.Value.Lazy
  List<BuiltSelect> selectQueries() {
    return queries().stream()
        .map(q -> (BuiltSelect) q.source().query())
        .collect(Collectors.toList());
  }

  @org.immutables.value.Value.Lazy
  AbstractTable table() {
    Set<AbstractTable> tables =
        selectQueries().stream().map(BuiltSelect::table).collect(Collectors.toSet());

    if (tables.isEmpty()) {
      throw new IllegalArgumentException("No tables are referenced by the provided queries");
    }

    if (tables.size() > 1) {
      throw new IllegalArgumentException(
          "Too many tables are referenced by the provided queries: "
              + tables.stream().map(AbstractTable::name).collect(Collectors.joining(", ")));
    }

    return tables.iterator().next();
  }

  @org.immutables.value.Value.Lazy
  Set<Column> selectedColumns() {
    Set<Set<Column>> sets =
        selectQueries().stream().map(BuiltSelect::selectedColumns).collect(Collectors.toSet());

    if (sets.size() != 1) {
      throw new IllegalArgumentException(
          "Incompatible sets of columns are selected by the provided queries: "
              + sets.stream()
                  .map(s -> s.stream().map(Column::name).collect(Collectors.joining(",", "[", "]")))
                  .collect(Collectors.joining("; ", "[", "]")));
    }

    return sets.iterator().next();
  }

  private boolean isSelected(Column column) {
    // An empty selection set means `*` (all columns)
    return selectedColumns().isEmpty() || selectedColumns().contains(column);
  }

  /** Retrurn the list of columns whose values are used to distinguish one document from another. */
  public List<Column> docIdColumns(int keyDepth) {
    if (keyDepth < table().partitionKeyColumns().size()
        || keyDepth > table().primaryKeyColumns().size()) {
      throw new IllegalArgumentException("Invalid document identity depth: " + keyDepth);
    }

    List<Column> idColumns = table().primaryKeyColumns().subList(0, keyDepth);

    idColumns.forEach(
        column -> {
          if (!isSelected(column)) {
            throw new IllegalArgumentException(
                "Required identity column is not selected: " + column);
          }
        });

    return idColumns;
  }

  /**
   * Returns a sub-list of the {@link #table() table's} clustering key columns that are explicitly
   * selected by the {@link #queries()}.
   */
  @org.immutables.value.Value.Lazy
  List<Column> docPathColumns() {
    return table().clusteringKeyColumns().stream()
        .filter(this::isSelected)
        .collect(Collectors.toList());
  }
}
