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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.types;

import graphql.schema.GraphQLList;
import graphql.schema.GraphQLModifiedType;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLType;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.SchemaWarningException;

public class TypeBuilder {
  /** Gets the name of a given GraphQL type to be used in composite type names. */
  protected String getTypeName(GraphQLType type) {
    if (type instanceof GraphQLNamedType) {
      return ((GraphQLNamedType) type).getName();
    }

    String modifier = "";
    if (type instanceof GraphQLList) {
      modifier = "List";
    }

    if (!(type instanceof GraphQLModifiedType)) {
      throw new SchemaWarningException(
          String.format("GraphQL type %s not supported in composite type", type));
    }

    return modifier + getTypeName(((GraphQLModifiedType) type).getWrappedType());
  }
}
