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

import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;

/** Represents a value. */
public interface CellValue {
  /** Gets the raw value of a cell. */
  ByteBuffer getValue();

  /** Uses a codec to provide the Java representation of the stored value. */
  Object getValueObject();

  /** Gets the column information associated with this cell value. */
  Column getColumn();
}
