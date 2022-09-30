/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.query.executor;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.model.paging.CombinedPagingState;
import io.stargate.sgv2.docsapi.service.query.model.paging.PagingStateSupplier;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Represents individual row that provides a value for document properties.
 *
 * <p>This class links a {@link RowWrapper} to its source result set and the {@link #queryIndex()
 * query} that produced it.
 *
 * @author Dmitri Bourlatchkov
 * @author Ivan Senic
 */
@Value.Immutable(lazyhash = true)
public abstract class DocumentProperty implements PagingStateSupplier {

  /**
   * A result set that points to a page where the row that describes this document property
   * originates from.
   */
  abstract QueryOuterClass.ResultSet page();

  /** A single result set row wrapped in the {@link RowWrapper}. */
  abstract RowWrapper rowWrapper();

  /** Indicates whether the associated row was the last row in its page. */
  abstract boolean lastInPage();

  /**
   * The index of the query that produced this row in the list of executed queries.
   *
   * <p>This index corresponds to the position of the query's paging state in the combined paging
   * state.
   */
  abstract int queryIndex();

  /**
   * @return Comparable key that can be used for comparing the partition belonging for this row. Or
   *     <code>null</code> if row does not contain it.
   */
  @Value.Lazy()
  @Nullable
  ByteString comparableKey() {
    QueryOuterClass.Row row = rowWrapper().row();

    if (row.hasComparableBytes()) {
      return row.getComparableBytes().getValue();
    } else {
      return null;
    }
  }

  /**
   * Returns a value of the column from the {@link #rowWrapper()}.
   *
   * @param column column name
   * @return Value or null if the column does not exist
   */
  String keyValue(String column) {
    if (rowWrapper().columnExists(column)) {
      return rowWrapper().getString(column);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <ol>
   *   <li>If last in page and the page does not have paging state, returns {@link
   *       CombinedPagingState#EXHAUSTED_PAGE_STATE}
   *   <li>If not last in page, but does not have a paging state in the wrapper {@link
   *       io.stargate.bridge.proto.QueryOuterClass.Row}, throws exception with {@link
   *       ErrorCode#DOCS_API_SEARCH_ROW_PAGE_STATE_MISSING}
   * </ol>
   */
  @Override
  public ByteBuffer makePagingState() {
    // Note: is current row was not the last one in its page the higher-level request may
    // choose to resume fetching from the next row in current page even if the page itself does
    // not have a paging state. Therefore, in this case we cannot return EXHAUSTED_PAGE_STATE.
    if (lastInPage() && !page().hasPagingState()) {
      return CombinedPagingState.EXHAUSTED_PAGE_STATE;
    }

    // ensure we have a row page state
    QueryOuterClass.Row row = rowWrapper().row();
    if (!row.hasPagingState()) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_ROW_PAGE_STATE_MISSING);
    }

    BytesValue pagingState = row.getPagingState();
    return pagingState.getValue().asReadOnlyByteBuffer();
  }

  @Value.Lazy
  @Override
  public String toString() {
    return rowWrapper().toString();
  }
}
