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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stargate.db;

import io.stargate.db.ImmutablePagingPosition.Builder;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Represents a custom paging position for requesting data from paginated queries. The difference
 * from the normal {@link ResultMetadata#pagingState paging state} is that this position may be set
 * based on a row in the middle of a page returned by the previous query execution.
 */
@Value.Immutable
public interface PagingPosition {

  static ImmutablePagingPosition.Builder builder() {
    return ImmutablePagingPosition.builder();
  }

  static ImmutablePagingPosition.Builder ofCurrentRow(Row row) {
    Builder builder = ImmutablePagingPosition.builder();
    for (Column c : row.columns()) {
      ByteBuffer value = row.getBytesUnsafe(c.name());
      if (value != null) {
        builder.putCurrentRow(c, value);
      }
    }

    return builder;
  }

  /** The reference row for the paging state. */
  Map<Column, ByteBuffer> currentRow();

  @Value.Lazy
  default Map<String, ByteBuffer> currentRowValuesByColumnName() {
    return currentRow().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().name(), Entry::getValue));
  }

  @Value.Lazy
  default TableName tableName() {
    return TableName.of(currentRow().keySet());
  }

  default ByteBuffer requiredValue(String columnName) {
    TableName table = tableName(); // indirectly validates that there's only one table referenced

    ByteBuffer value = currentRowValuesByColumnName().get(columnName);

    if (value == null) {
      throw new IllegalArgumentException(
          String.format(
              "Required value is not present in current row (table: %s, column: %s)",
              table.name(), columnName));
    }

    return value;
  }

  /** Defines how to resume paging relative to the {@link #currentRow() reference row}. */
  ResumeMode resumeFrom();

  /**
   * The maximum number of rows to be returned by the query execution requests using this paging
   * position.
   *
   * <p>Note: since the custom paging position may be chosen arbitrarily, the caller is responsible
   * for tracking query limits and calculating the number of rows remaining.
   */
  @Value.Default
  default int remainingRows() {
    return Integer.MAX_VALUE;
  }

  /**
   * The maximum number of rows to be returned for next data partition in the query execution
   * requests using this paging position.
   *
   * <p>Note: since the custom paging position may be chosen arbitrarily, the caller is responsible
   * for tracking per-partition query limits and calculating the number of rows remaining in next
   * partition.
   *
   * <p>Note: if {@link ResumeMode#NEXT_PARTITION} is used, this parameter does not have to be set
   * (the per-partition limit will be inherited from the query).
   */
  @Value.Default
  default int remainingRowsInPartition() {
    return Integer.MAX_VALUE;
  }

  enum ResumeMode {
    /**
     * Paging is resumed from the first row of the partition following the partition of the {@link
     * #currentRow() reference row}.
     */
    NEXT_PARTITION,

    /** Paging is resumed from the row following the {@link #currentRow() reference row}. */
    NEXT_ROW,
  }
}
