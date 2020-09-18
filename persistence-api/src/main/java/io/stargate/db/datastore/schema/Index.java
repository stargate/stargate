/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.datastore.schema;

import io.stargate.db.datastore.query.ColumnOrder;
import io.stargate.db.datastore.query.WhereCondition;
import java.util.List;
import java.util.OptionalLong;

public interface Index extends SchemaEntity {
  /** @return true if the query can be supported by the index */
  boolean supports(
      List<Column> select,
      List<WhereCondition<?>> conditions,
      List<ColumnOrder> orders,
      OptionalLong limit);

  /**
   * @return The priority of this class of index where a lower value indicates a higher priority.
   */
  int priority();

  String indexTypeName();
}
