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
package io.stargate.graphql.schema.cqlfirst.dml;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLType;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.cqlfirst.dml.types.MapBuilder;
import io.stargate.graphql.schema.cqlfirst.dml.types.TupleBuilder;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Caches GraphQL field input types, for example 'String' in:
 *
 * <pre>
 * input BooksInput {
 *   author: String
 *   title: String
 * }
 * </pre>
 *
 * These are used when receiving data, for in an 'insertBooks' mutation.
 */
class FieldInputTypeCache extends FieldTypeCache<GraphQLInputType> {

  private final List<String> warnings;

  FieldInputTypeCache(NameMapping nameMapping, List<String> warnings) {
    super(nameMapping);
    this.warnings = warnings;
  }

  @Override
  protected GraphQLInputType compute(Column.ColumnType columnType) {
    if (columnType.isMap()) {
      GraphQLType keyType = get(columnType.parameters().get(0));
      GraphQLType valueType = get(columnType.parameters().get(1));
      return new MapBuilder(keyType, valueType, true).build();
    } else if (columnType.isList() || columnType.isSet()) {
      return new GraphQLList(get(columnType.parameters().get(0)));
    } else if (columnType.isUserDefined()) {
      UserDefinedType udt = (UserDefinedType) columnType;
      return computeUdt(udt);
    } else if (columnType.isTuple()) {
      List<GraphQLType> subTypes =
          columnType.parameters().stream().map(this::get).collect(Collectors.toList());
      return new TupleBuilder(subTypes).buildInputType();
    } else {
      return getScalar(columnType.rawType());
    }
  }

  private GraphQLInputType computeUdt(UserDefinedType udt) {
    String graphqlName = nameMapping.getGraphqlName(udt);
    if (graphqlName == null) {
      throw new SchemaWarningException(
          String.format(
              "Could not find a GraphQL name mapping for UDT %s, "
                  + "this is probably because it clashes with another UDT",
              udt.name()));
    }
    GraphQLInputObjectType.Builder builder =
        GraphQLInputObjectType.newInputObject().name(graphqlName + "Input");
    for (Column column : udt.columns()) {
      String graphqlFieldName = nameMapping.getGraphqlName(udt, column);
      if (graphqlFieldName != null) {
        try {
          builder.field(
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlFieldName)
                  .type(get(column.type()))
                  .build());
        } catch (Exception e) {
          warnings.add(
              String.format(
                  "Could not create input type for field %s in UDT %s, skipping (%s)",
                  column.name(), column.table(), e.getMessage()));
        }
      }
    }
    return builder.build();
  }
}
