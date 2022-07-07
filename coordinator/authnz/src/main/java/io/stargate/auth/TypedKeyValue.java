package io.stargate.auth;

import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.PartitionKey;
import io.stargate.db.query.PrimaryKey;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.RowsRange;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class TypedKeyValue {

  private final String name;
  private final ColumnType type;
  private final Object value;

  public TypedKeyValue(String name, ColumnType type, Object value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  public TypedKeyValue(String name, TypedValue typedValue) {
    this(name, typedValue.type(), typedValue.javaValue());
  }

  public String getName() {
    return name;
  }

  public ColumnType getType() {
    return type;
  }

  public Object getValue() {
    return value;
  }

  private static List<TypedKeyValue> fromImpactedRows(
      AbstractTable table, RowsImpacted rowsSelection) {
    List<Column> keyColumns = table.primaryKeyColumns();
    List<TypedKeyValue> typedKeyValues = new ArrayList<>();
    if (rowsSelection.isKeys()) {
      for (PrimaryKey key : rowsSelection.asKeys().primaryKeys()) {
        for (int i = 0; i < keyColumns.size(); i++) {
          typedKeyValues.add(new TypedKeyValue(keyColumns.get(i).name(), key.get(i)));
        }
      }
    } else {
      for (RowsRange range : rowsSelection.asRanges().ranges()) {
        PartitionKey partitionKey = range.partitionKey();
        for (int i = 0; i < partitionKey.size(); i++) {
          typedKeyValues.add(new TypedKeyValue(keyColumns.get(i).name(), partitionKey.get(i)));
        }
        // TODO: should we include the rest of the bounds?
      }
    }
    return typedKeyValues;
  }

  public static List<TypedKeyValue> forSelect(BoundSelect select) {
    Optional<RowsImpacted> selected = select.selectedRows();
    return selected
        .map(rowsImpacted -> fromImpactedRows(select.table(), rowsImpacted))
        .orElse(Collections.emptyList());
  }

  public static List<TypedKeyValue> forDML(BoundDMLQuery dmlQuery) {
    return fromImpactedRows(dmlQuery.table(), dmlQuery.rowsUpdated());
  }
}
