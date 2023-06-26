package io.stargate.db.query;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.util.List;
import java.util.Objects;

public abstract class SchemaKey {

  protected final AbstractTable table;
  protected final List<TypedValue> values;

  public SchemaKey(AbstractTable table, List<TypedValue> values) {
    this.table = table;
    this.values = values;

    List<Column> keyColumns = allColumns();
    Preconditions.checkArgument(
        keyColumns.size() == values.size(),
        "Expected %s %s values, but only %s provided",
        keyColumns.size(),
        keyName(),
        values.size());

    for (int i = 0; i < keyColumns.size(); i++) {
      ColumnType expectedType = keyColumns.get(i).type();
      assert expectedType != null;
      ColumnType actualType = values.get(i).type();
      Preconditions.checkArgument(
          expectedType.equals(actualType),
          "Invalid type for value %s: expecting %s but got %s",
          i,
          expectedType,
          actualType);
    }
  }

  protected abstract String keyName();

  protected abstract int columnIndex(Column column);

  public int size() {
    return values.size();
  }

  public TypedValue get(int i) {
    return values.get(i);
  }

  public TypedValue get(String name) {
    return get(table.existingColumn(name));
  }

  public TypedValue get(Column column) {
    return values.get(columnIndex(column));
  }

  public abstract List<Column> allColumns();

  public List<TypedValue> allValues() {
    return values;
  }

  @Override
  @SuppressWarnings("EqualsGetClass")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaKey that = (SchemaKey) o;
    return table.keyspace().equals(that.table.keyspace())
        && table.name().equals(that.table.name())
        && values.equals(that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table.keyspace(), table.name(), values);
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
