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
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;

public class DefaultCell extends DefaultCellValue implements Cell {

  private final int ttl;
  private final Modification.Operation operation;

  public DefaultCell(
      ByteBuffer value,
      Object valueObject,
      Column column,
      int ttl,
      Modification.Operation operation) {
    super(value, valueObject, column);
    this.ttl = ttl;
    this.operation = operation;
  }

  @Override
  public int getTTL() {
    return ttl;
  }

  @Override
  public Modification.Operation operation() {
    return operation;
  }
}
