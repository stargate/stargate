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

import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import java.nio.ByteBuffer;

/**
 * A table-specific interface for extracting key column values from a {@link ResultSet} {@link Row}
 * in a comparable form.
 */
public interface RowDecorator {

  /**
   * Decorates the partition key of the {@link Row} such that the resultant objects compare in the
   * same order that queries iterate / paginate over the Cassandra data ring.
   */
  <T extends Comparable<T>> ComparableKey<T> decoratePartitionKey(Row row);

  ByteBuffer getComparableBytes(Row row);
}
