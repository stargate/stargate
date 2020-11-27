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
package io.stargate.api.sql.server.postgres;

import java.util.Arrays;

public enum PGType {
  Bool(16, 1),
  Date(1082, 4),
  Time(1083, 8),
  Timestamp(1114, 8),
  Float8(701, 8),
  Int2(21, 2),
  Int4(23, 4),
  Int8(20, 8),
  Numeric(1700, -1),
  Varchar(1043, -1);

  private final int oid;
  private final int length;

  PGType(int oid, int length) {
    this.oid = oid;
    this.length = length;
  }

  static PGType of(int oid) {
    return Arrays.stream(values())
        .filter(t -> t.oid == oid)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Unsupported type OID: " + oid));
  }

  public int oid() {
    return oid;
  }

  public int length() {
    return length;
  }
}
