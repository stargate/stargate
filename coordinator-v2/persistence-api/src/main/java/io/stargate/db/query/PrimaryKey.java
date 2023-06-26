package io.stargate.db.query;

import io.stargate.db.schema.AbstractTable;
import io.stargate.db.schema.Column;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A row primary key.
 *
 * <p>A primary key object is essentially a list of {@link TypedValue}, but it guarantees that the
 * number of elements in that list is equals to the number of primary keys for the table this is a
 * primary key of, and that the order of values corresponds to those keys as well.
 */
public class PrimaryKey extends SchemaKey {
  private @Nullable PartitionKey partitionKey; // Lazily computed

  public PrimaryKey(AbstractTable table, List<TypedValue> values) {
    super(table, values);
  }

  public PartitionKey partitionKey() {
    if (partitionKey == null) {
      partitionKey = new PartitionKey(table, values.subList(0, table.partitionKeyColumns().size()));
    }
    return partitionKey;
  }

  @Override
  protected String keyName() {
    return "primary key";
  }

  @Override
  protected int columnIndex(Column column) {
    return table.primaryKeyColumnIndex(column);
  }

  @Override
  public List<Column> allColumns() {
    return table.primaryKeyColumns();
  }
}
