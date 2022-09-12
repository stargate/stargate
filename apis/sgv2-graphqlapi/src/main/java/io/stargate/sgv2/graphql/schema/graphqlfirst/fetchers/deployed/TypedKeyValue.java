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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.Value;

/**
 * A column definition and its value (used to represent primary keys in {@link MutationPayload}).
 */
class TypedKeyValue {

  private final String name;
  private final TypeSpec type;
  private final Value value;

  TypedKeyValue(String name, TypeSpec type, Value value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }

  String getName() {
    return name;
  }

  TypeSpec getType() {
    return type;
  }

  Value getValue() {
    return value;
  }
}
