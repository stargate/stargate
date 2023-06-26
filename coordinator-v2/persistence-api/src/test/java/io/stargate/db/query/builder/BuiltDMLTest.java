package io.stargate.db.query.builder;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.ImmutableModification;
import io.stargate.db.query.ModifiableEntity;
import io.stargate.db.query.Modification;
import io.stargate.db.query.Modification.Operation;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import io.stargate.db.schema.Column;

public abstract class BuiltDMLTest<B extends BoundDMLQuery> extends BuiltQueryTest {

  protected B bound;

  protected BuiltDMLTest(QueryType expectedQueryType) {
    super(expectedQueryType);
  }

  protected void setBound(BoundQuery bound) {
    this.bound = checkedCast(bound);
    assertThat(this.bound.table().keyspace()).isEqualTo(KS_NAME);
  }

  private void checkBoundSet() {
    // Note: it's a programming error in the test, not a test failure.
    Preconditions.checkState(bound != null, "The setBound() method should have been called before");
  }

  protected Column column(String columnName) {
    checkBoundSet();
    Column column = bound.table().column(columnName);
    // Note: it's a programming error in the test, not a test failure.
    Preconditions.checkArgument(column != null, "Cannot use non-existing column %s", columnName);
    return column;
  }

  protected Modification set(String columnName, Object value) {
    Column column = column(columnName);
    return ImmutableModification.builder()
        .entity(ModifiableEntity.of(column))
        .operation(Operation.SET)
        .value(TypedValue.forJavaValue(codec(), column.name(), column.type(), value))
        .build();
  }
}
