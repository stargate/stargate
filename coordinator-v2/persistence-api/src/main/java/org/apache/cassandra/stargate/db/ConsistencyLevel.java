/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.db;

import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.transport.ProtocolException;

public enum ConsistencyLevel {
  ANY(0),
  ONE(1),
  TWO(2),
  THREE(3),
  QUORUM(4),
  ALL(5),
  LOCAL_QUORUM(6, true),
  EACH_QUORUM(7),
  SERIAL(8),
  LOCAL_SERIAL(9),
  LOCAL_ONE(10, true),
  NODE_LOCAL(11, true);

  // Used by the binary protocol
  public final int code;
  private final boolean isDCLocal;
  private static final ConsistencyLevel[] codeIdx;

  static {
    int maxCode = -1;
    for (ConsistencyLevel cl : ConsistencyLevel.values()) {
      maxCode = Math.max(maxCode, cl.code);
    }
    codeIdx = new ConsistencyLevel[maxCode + 1];
    for (ConsistencyLevel cl : ConsistencyLevel.values()) {
      if (codeIdx[cl.code] != null) {
        throw new IllegalStateException("Duplicate code");
      }
      codeIdx[cl.code] = cl;
    }
  }

  ConsistencyLevel(int code) {
    this(code, false);
  }

  ConsistencyLevel(int code, boolean isDCLocal) {
    this.code = code;
    this.isDCLocal = isDCLocal;
  }

  public static ConsistencyLevel fromCode(int code) throws ProtocolException {
    if (code < 0 || code >= codeIdx.length) {
      throw new ProtocolException(String.format("Unknown code %d for a consistency level", code));
    }
    return codeIdx[code];
  }

  public boolean isDatacenterLocal() {
    return isDCLocal;
  }

  public void validateForRead(String keyspaceName) throws InvalidRequestException {
    if (this == ConsistencyLevel.ANY) {
      throw new InvalidRequestException("ANY ConsistencyLevel is only supported for writes");
    }
  }

  public void validateForWrite(String keyspaceName) throws InvalidRequestException {
    if (this == ConsistencyLevel.SERIAL || this == ConsistencyLevel.LOCAL_SERIAL) {
      throw new InvalidRequestException("You must use conditional updates for serializable writes");
    }
  }

  public boolean isSerialConsistency() {
    return this == SERIAL || this == LOCAL_SERIAL;
  }
}
