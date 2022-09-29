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
package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import static graphql.schema.GraphQLList.list;
import static io.stargate.graphql.schema.cqlfirst.dml.fetchers.DataTypeMapping.toDBValue;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputType;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.BuiltCondition.LHS;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.graphql.schema.cqlfirst.dml.NameMapping;
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
    protected Object conditionValue(
        ColumnType type, Object graphCQLValue, NameMapping nameMapping) {
      if (graphCQLValue instanceof Collection<?>) {
        Collection<?> values = (Collection<?>) graphCQLValue;
        return values.stream()
            .map(item -> toDBValue(type, item, nameMapping))
            .collect(Collectors.toList());
      }
      return Collections.singletonList(toDBValue(type, graphCQLValue, nameMapping));
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
    protected Object conditionValue(
        ColumnType type, Object graphCQLValue, NameMapping nameMapping) {
      assert type.isCollection();
      ColumnType elementType = type.parameters().get(type.isMap() ? 1 : 0);
      return toDBValue(elementType, graphCQLValue, nameMapping);
    }
  },
  CONTAINS_KEY("containsKey", Predicate.CONTAINS_KEY) {
    @Override
    protected Object conditionValue(
        ColumnType type, Object graphCQLValue, NameMapping nameMapping) {
      assert type.isMap();
      ColumnType keyType = type.parameters().get(0);
      return toDBValue(keyType, graphCQLValue, nameMapping);
    }
  },
  CONTAINS_ENTRY("containsEntry", Predicate.EQ) {
    @SuppressWarnings("unchecked")
    private Map<String, Object> entry(Object graphCQLValue) {
      return (Map<String, Object>) graphCQLValue;
    }

    @Override
    protected LHS conditionLHS(Column column, Object graphCQLValue, NameMapping nameMapping) {
      Object key =
          toDBValue(
              column.type().parameters().get(0), entry(graphCQLValue).get("key"), nameMapping);
      return LHS.mapAccess(column.name(), key);
    }

    @Override
    protected Object conditionValue(
        ColumnType type, Object graphCQLValue, NameMapping nameMapping) {
      return toDBValue(type.parameters().get(1), entry(graphCQLValue).get("value"), nameMapping);
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

  public BuiltCondition buildCondition(Column column, Object value, NameMapping nameMapping) {
    ColumnType type = column.type();
    assert type != null;
    return BuiltCondition.of(
        conditionLHS(column, value, nameMapping),
        predicate,
        conditionValue(type, value, nameMapping));
  }

  protected LHS conditionLHS(Column column, Object graphCQLValue, NameMapping nameMapping) {
    return LHS.column(column.name());
  }

  protected Object conditionValue(ColumnType type, Object graphCQLValue, NameMapping nameMapping) {
    return toDBValue(type, graphCQLValue, nameMapping);
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
