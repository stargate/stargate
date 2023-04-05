package io.stargate.db.query.builder;

import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.QueryType;
import io.stargate.db.schema.Column;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class BuiltCreateTest extends BuiltDMLTest<BoundDelete> {

  BuiltCreateTest() {
    super(QueryType.OTHER);
  }

  @Nested
  class Table {

    @Test
    public void withComment() {
      QueryBuilder builder = newBuilder();

      BuiltQuery<?> query =
          builder
              .create()
              .table(KS_NAME, "new_table")
              .column(Column.create("key", Column.Kind.PartitionKey, Column.Type.Text))
              .withComment("Special Ivan's comment.")
              .build();

      assertBuiltQuery(
          query,
          "CREATE TABLE ks.new_table (key text, PRIMARY KEY ((key))) WITH comment = 'Special Ivan''s comment.'");
    }
  }
}
