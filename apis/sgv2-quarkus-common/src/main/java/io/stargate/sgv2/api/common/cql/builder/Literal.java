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
package io.stargate.sgv2.api.common.cql.builder;

import io.stargate.bridge.proto.QueryOuterClass.Value;
import java.util.Objects;

public class Literal implements Term {

  static final String NULL_ERROR_MESSAGE = "Use Values.NULL to bind a null CQL value";

  private final Value value;

  public Literal(Value value) {
    this.value = Objects.requireNonNull(value, NULL_ERROR_MESSAGE);
  }

  public Value get() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Literal) {
      Literal that = (Literal) other;
      return Objects.equals(this.value, that.value);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
