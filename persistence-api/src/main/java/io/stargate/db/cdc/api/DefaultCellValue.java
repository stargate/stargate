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

public class DefaultCellValue implements CellValue {

  private final ByteBuffer value;
  private final Object valueObject;
  private final Column column;

  public DefaultCellValue(ByteBuffer value, Object valueObject, Column column) {
    this.value = value;
    this.valueObject = valueObject;
    this.column = column;
  }

  @Override
  public ByteBuffer getValue() {
    return value;
  }

  @Override
  public Object getValueObject() {
    return valueObject;
  }

  @Override
  public Column getColumn() {
    return column;
  }
}
