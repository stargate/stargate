package io.stargate.db.query.builder;

import io.stargate.db.query.BoundDelete;
import io.stargate.db.query.QueryType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class BuiltAlterTest extends BuiltDMLTest<BoundDelete> {

  BuiltAlterTest() {
    super(QueryType.OTHER);
  }

  @Nested
  class Table {

    @Test
    public void withComment() {
      BuiltQuery<?> query =
          newBuilder().alter().table(KS_NAME, "t1").withComment("Special Ivan's comment.").build();

      assertBuiltQuery(query, "ALTER TABLE ks.t1 WITH comment = 'Special Ivan''s comment.'");
    }
  }
}
