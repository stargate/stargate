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

import org.apache.cassandra.stargate.transport.ProtocolException;

public enum BatchType {
  LOGGED(0),
  UNLOGGED(1),
  COUNTER(2);

  public final int id;

  BatchType(int id) {
    this.id = id;
  }

  public static BatchType fromId(int id) {
    for (BatchType t : values()) {
      if (t.id == id) return t;
    }
    throw new ProtocolException(String.format("Unknown batch type %d", id));
  }
}
