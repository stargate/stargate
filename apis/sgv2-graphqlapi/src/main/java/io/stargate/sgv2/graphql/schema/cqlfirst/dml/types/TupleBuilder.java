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
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TupleBuilder extends TypeBuilder {
  private final List<GraphQLType> subTypes;

  public TupleBuilder(List<GraphQLType> subTypes) {
    this.subTypes = subTypes;
  }

  private String getName(boolean isInput) {
    return String.format(
        "Tuple%s%s",
        subTypes.stream().map(this::getTypeName).collect(Collectors.joining()),
        isInput ? "Input" : "");
  }

  public GraphQLOutputType buildOutputType() {
    List<GraphQLFieldDefinition> fields = new ArrayList<>(subTypes.size());

    for (int i = 0; i < subTypes.size(); i++) {
      fields.add(
          GraphQLFieldDefinition.newFieldDefinition()
              .name(String.format("item%d", i))
              .type((GraphQLOutputType) subTypes.get(i))
              .build());
    }

    return GraphQLObjectType.newObject()
        .description(String.format("Represents a Tuple type with %d items", subTypes.size()))
        .name(getName(false))
        .fields(fields)
        .build();
  }

  public GraphQLInputType buildInputType() {
    List<GraphQLInputObjectField> fields = new ArrayList<>(subTypes.size());

    for (int i = 0; i < subTypes.size(); i++) {
      fields.add(
          GraphQLInputObjectField.newInputObjectField()
              .name(String.format("item%d", i))
              .type((GraphQLInputType) subTypes.get(i))
              .build());
    }

    return GraphQLInputObjectType.newInputObject()
        .description(String.format("Represents a Tuple input type with %d items", subTypes.size()))
        .name(getName(true))
        .fields(fields)
        .build();
  }
}
