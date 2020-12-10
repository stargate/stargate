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
package io.stargate.db.cdc.api;

import io.stargate.db.query.Modification;

/** Represents a row cell containing a value, for regular columns. */
public interface Cell extends CellValue {
  /**
   * The cell ttl.
   *
   * @return the cell ttl, or {@code 0} if the cell isn't an expiring one.
   */
  int getTTL();

  /** operation type for a specific cell */
  Modification.Operation operation();
}
