package io.stargate.bridge.service.docsapi;

import io.stargate.db.ComparableKey;
import io.stargate.db.PagingPosition;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.proto.QueryOuterClass;
import java.nio.ByteBuffer;

/**
 * Represents individual rows that provide values for document properties.
 *
 * <p>This class links a {@link QueryOuterClass.Row} to its source {@link Page} and the {@link
 * #queryIndex() query} that produced it.
 */
@org.immutables.value.Value.Immutable(lazyhash = true)
public abstract class DocProperty implements PagingStateSupplier {

  abstract Page page();

  abstract Row row();

  /** Indicates whether the associated row was the last row in its page. */
  abstract boolean lastInPage();

  /**
   * The index of the query that produced this row in the list of executed queries.
   *
   * <p>This index corresponds to the position of the query's paging state in the combined paging
   * state.
   */
  abstract int queryIndex();

  @org.immutables.value.Value.Lazy
  ComparableKey<?> comparableKey() {
    return page().decorator().decoratePartitionKey(row());
  }

  String keyValue(Column column) {
    return row().getString(column.name());
  }

  @Override
  public ByteBuffer makePagingState(ResumeMode resumeMode) {
    // Note: is current row was not the last one in its page the higher-level request may
    // choose to resume fetching from the next row in current page even if the page itself does
    // not have a paging state. Therefore, in this case we cannot return EXHAUSTED_PAGE_STATE.
    if (lastInPage() && page().resultSet().getPagingState() == null) {
      return CombinedPagingState.EXHAUSTED_PAGE_STATE;
    }

    return page()
        .resultSet()
        .makePagingState(PagingPosition.ofCurrentRow(row()).resumeFrom(resumeMode).build());
  }

  @org.immutables.value.Value.Lazy
  @Override
  public String toString() {
    return row().toString();
  }
}
