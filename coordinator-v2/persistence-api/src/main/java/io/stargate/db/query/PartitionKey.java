package io.stargate.db.query;

import static java.lang.String.format;

import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import java.util.List;

/**
 * A partition key, identifying a particular partition.
 *
 * <p>A partition key object is essentially a list of {@link TypedValue}, but it guarantees that the
 * number of elements in that list is equals to the number of partition keys for the table this is a
 * primary key of, and that the order of values corresponds to those keys as well.
 */
public class PartitionKey extends SchemaKey {

  public PartitionKey(AbstractTable table, List<TypedValue> values) {
    super(table, values);
  }

  @Override
  protected String keyName() {
    return "partition key";
  }

  @Override
  protected int columnIndex(Column column) {
    int idx = table.primaryKeyColumnIndex(column);
    if (idx >= table.partitionKeyColumns().size()) {
      throw new IllegalArgumentException(
          format(
              "Column %s is not a partition key column of %s.%s",
              column, table.cqlKeyspace(), table.cqlName()));
    }
    return idx;
  }

  @Override
  public List<Column> allColumns() {
    return table.partitionKeyColumns();
  }
}
