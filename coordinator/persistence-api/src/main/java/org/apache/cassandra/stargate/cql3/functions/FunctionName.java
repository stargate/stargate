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
package org.apache.cassandra.stargate.cql3.functions;

import java.util.Objects;

public final class FunctionName {
  public final String keyspace;
  public final String name;

  public FunctionName(String keyspace, String name) {
    assert name != null : "Name parameter must not be null";
    this.keyspace = keyspace;
    this.name = name;
  }

  public boolean hasKeyspace() {
    return keyspace != null;
  }

  @Override
  public final int hashCode() {
    return Objects.hash(keyspace, name);
  }

  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof FunctionName)) {
      return false;
    }

    FunctionName that = (FunctionName) o;
    return Objects.equals(this.keyspace, that.keyspace) && Objects.equals(this.name, that.name);
  }

  @Override
  public String toString() {
    return keyspace == null ? name : keyspace + "." + name;
  }
}
