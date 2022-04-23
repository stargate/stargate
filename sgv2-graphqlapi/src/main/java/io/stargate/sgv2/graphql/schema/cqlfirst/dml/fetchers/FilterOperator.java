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
package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static graphql.schema.GraphQLList.list;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputType;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.BuiltCondition.LHS;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.NameMapping;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/** Represents the relational and equality operators for filtering in DML GraphQL queries. */
public enum FilterOperator {
  EQUAL("eq", Predicate.EQ),
  NOT_EQUAL("notEq", Predicate.NEQ),
  GREATER_THAN("gt", Predicate.GT),
  GREATER_THAN_EQUAL("gte", Predicate.GTE),
  LESS_THAN("lt", Predicate.LT),
  LESS_THAN_EQUAL("lte", Predicate.LTE),
  IN("in", Predicate.IN) {
    @Override
    protected Object conditionValue(TypeSpec type, Object graphCQLValue, NameMapping nameMapping) {
      if (graphCQLValue instanceof Collection<?>) {
        Collection<?> values = (Collection<?>) graphCQLValue;
        Value[] elements =
            values.stream()
                .map(item -> DataTypeMapping.toGrpcValue(type, item, nameMapping))
                .toArray(Value[]::new);
        return Values.of(elements);
      }
      return Collections.singletonList(
          DataTypeMapping.toGrpcValue(type, graphCQLValue, nameMapping));
    }

    @Override
    public GraphQLInputObjectField buildField(GraphQLInputType gqlInputType) {
      return GraphQLInputObjectField.newInputObjectField()
          .name(getFieldName())
          .type(list(gqlInputType))
          .build();
    }
  },
  CONTAINS("contains", Predicate.CONTAINS) {
    @Override
    protected Object conditionValue(TypeSpec type, Object graphCQLValue, NameMapping nameMapping) {
      TypeSpec elementType;
      switch (type.getSpecCase()) {
        case LIST:
          elementType = type.getList().getElement();
          break;
        case SET:
          elementType = type.getSet().getElement();
          break;
        case MAP:
          elementType = type.getMap().getValue();
          break;
        default:
          throw new AssertionError(
              "Should not attempt to use CONTAINS with type " + type.getSpecCase());
      }
      return DataTypeMapping.toGrpcValue(elementType, graphCQLValue, nameMapping);
    }
  },
  CONTAINS_KEY("containsKey", Predicate.CONTAINS_KEY) {
    @Override
    protected Object conditionValue(TypeSpec type, Object graphCQLValue, NameMapping nameMapping) {
      if (type.getSpecCase() != TypeSpec.SpecCase.MAP) {
        throw new AssertionError(
            "Should not attempt to use CONTAINS_KEY with type " + type.getSpecCase());
      }
      TypeSpec keyType = type.getMap().getKey();
      return DataTypeMapping.toGrpcValue(keyType, graphCQLValue, nameMapping);
    }
  },
  CONTAINS_ENTRY("containsEntry", Predicate.EQ) {
    @SuppressWarnings("unchecked")
    private Map<String, Object> entry(Object graphCQLValue) {
      return (Map<String, Object>) graphCQLValue;
    }

    @Override
    protected LHS conditionLHS(ColumnSpec column, Object graphCQLValue, NameMapping nameMapping) {
      TypeSpec type = column.getType();
      if (type.getSpecCase() != TypeSpec.SpecCase.MAP) {
        throw new AssertionError(
            "Should not attempt to use CONTAINS_KEY with type " + type.getSpecCase());
      }
      TypeSpec keyType = type.getMap().getKey();
      Object key =
          DataTypeMapping.toGrpcValue(keyType, entry(graphCQLValue).get("key"), nameMapping);
      return LHS.mapAccess(column.getName(), key);
    }

    @Override
    protected Object conditionValue(TypeSpec type, Object graphCQLValue, NameMapping nameMapping) {
      // no assertion since we know conditionLHS is called first
      TypeSpec valueType = type.getMap().getValue();
      return DataTypeMapping.toGrpcValue(valueType, entry(graphCQLValue).get("value"), nameMapping);
    }
  },
  ;

  private static final Map<String, FilterOperator> mapByFieldName = buildMapByFieldName();
  private final String fieldName;
  private final Predicate predicate;

  public static FilterOperator fromFieldName(String fieldName) {
    FilterOperator op = mapByFieldName.get(fieldName);
    if (op == null) {
      throw new IllegalArgumentException("Invalid filter field name: " + fieldName);
    }
    return op;
  }

  private static Map<String, FilterOperator> buildMapByFieldName() {
    return Arrays.stream(FilterOperator.values())
        .collect(Collectors.toMap(FilterOperator::getFieldName, o -> o));
  }

  public BuiltCondition buildCondition(ColumnSpec column, Object value, NameMapping nameMapping) {
    TypeSpec type = column.getType();
    return BuiltCondition.of(
        conditionLHS(column, value, nameMapping),
        predicate,
        conditionValue(type, value, nameMapping));
  }

  protected LHS conditionLHS(ColumnSpec column, Object graphCQLValue, NameMapping nameMapping) {
    return LHS.column(column.getName());
  }

  protected Object conditionValue(TypeSpec type, Object graphCQLValue, NameMapping nameMapping) {
    return DataTypeMapping.toGrpcValue(type, graphCQLValue, nameMapping);
  }

  public GraphQLInputObjectField buildField(GraphQLInputType gqlInputType) {
    return GraphQLInputObjectField.newInputObjectField().name(fieldName).type(gqlInputType).build();
  }

  FilterOperator(String fieldName, Predicate predicate) {
    this.fieldName = fieldName;
    this.predicate = predicate;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Predicate predicate() {
    return predicate;
  }
}
