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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLType;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.types.MapBuilder;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.types.TupleBuilder;
import java.util.List;
import java.util.Map;
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
  protected GraphQLInputType compute(TypeSpec columnType) {
    switch (columnType.getSpecCase()) {
      case MAP:
        TypeSpec.Map map = columnType.getMap();
        return new MapBuilder(get(map.getKey()), get(map.getValue()), true).build();
      case LIST:
        return new GraphQLList(get(columnType.getList().getElement()));
      case SET:
        return new GraphQLList(get(columnType.getSet().getElement()));
      case UDT:
        return computeUdt(columnType.getUdt());
      case TUPLE:
        List<GraphQLType> subTypes =
            columnType.getTuple().getElementsList().stream()
                .map(this::get)
                .collect(Collectors.toList());
        return new TupleBuilder(subTypes).buildInputType();
      case BASIC:
        return getScalar(columnType.getBasic());
      default:
        throw new IllegalArgumentException("Unsupported type " + columnType);
    }
  }

  private GraphQLInputType computeUdt(Udt udt) {
    String graphqlName = nameMapping.getGraphqlName(udt);
    if (graphqlName == null) {
      throw new SchemaWarningException(
          String.format(
              "Could not find a GraphQL name mapping for UDT %s, "
                  + "this is probably because it clashes with another UDT",
              udt.getName()));
    }
    GraphQLInputObjectType.Builder builder =
        GraphQLInputObjectType.newInputObject().name(graphqlName + "Input");
    for (Map.Entry<String, TypeSpec> entry : udt.getFieldsMap().entrySet()) {
      String cqlFieldName = entry.getKey();
      TypeSpec fieldType = entry.getValue();
      String graphqlFieldName = nameMapping.getGraphqlName(udt, cqlFieldName);
      if (graphqlFieldName != null) {
        try {
          builder.field(
              GraphQLInputObjectField.newInputObjectField()
                  .name(graphqlFieldName)
                  .type(get(fieldType))
                  .build());
        } catch (Exception e) {
          warnings.add(
              String.format(
                  "Could not create input type for field %s in UDT %s, skipping (%s)",
                  cqlFieldName, udt.getName(), e.getMessage()));
        }
      }
    }
    return builder.build();
  }
}
