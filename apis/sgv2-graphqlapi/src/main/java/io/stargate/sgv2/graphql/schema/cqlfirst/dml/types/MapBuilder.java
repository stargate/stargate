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

import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;

public class MapBuilder extends TypeBuilder {
  private final GraphQLType keyType;
  private final GraphQLType valueType;
  private final boolean isInput;

  public MapBuilder(GraphQLType keyType, GraphQLType valueType, boolean isInput) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.isInput = isInput;
  }

  public GraphQLList build() {
    return new GraphQLList(buildKeyValueType());
  }

  /** Builds the child object type composed by key and value properties. */
  private GraphQLType buildKeyValueType() {
    String keyTypeName = getTypeName(keyType);
    String valueTypeName = getTypeName(valueType);
    String name =
        String.format("Entry%sKey%sValue%s", keyTypeName, valueTypeName, isInput ? "Input" : "");

    // Maps composed of the same sub types should be the same type
    return isInput ? buildInputType(name) : buildOutputType(name);
  }

  private GraphQLInputType buildInputType(String name) {
    return GraphQLInputObjectType.newInputObject()
        .name(name)
        .description("Represents a key/value type for a Map")
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("key")
                .type(new GraphQLNonNull(keyType))
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("value")
                .type((GraphQLInputType) valueType)
                .build())
        .build();
  }

  private GraphQLOutputType buildOutputType(String name) {
    return GraphQLObjectType.newObject()
        .description("Represents a key/value type for a Map")
        .name(name)
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("key")
                .type(new GraphQLNonNull(keyType))
                .build())
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("value")
                .type((GraphQLOutputType) valueType)
                .build())
        .build();
  }
}
