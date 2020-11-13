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
package io.stargate.graphql.schema.fetchers.dml;

import static graphql.schema.GraphQLList.list;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.condition.ConditionBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ArithmeticRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputType;
import io.stargate.db.schema.Column;
import io.stargate.graphql.schema.NameMapping;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Represents the relational and equality operators for filtering in DML GraphQL queries. */
public enum FilterOperator {
  EQUAL("eq") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleCondition(column, value, nameMapping, ArithmeticRelationBuilder::isEqualTo);
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleRelation(column, value, nameMapping, ArithmeticRelationBuilder::isEqualTo);
    }
  },
  NOT_EQUAL("notEq") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleCondition(
          column, value, nameMapping, ArithmeticRelationBuilder::isNotEqualTo);
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleRelation(
          column, value, nameMapping, ArithmeticRelationBuilder::isNotEqualTo);
    }
  },
  GREATER_THAN("gt") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return FilterOperator.buildSimpleCondition(
          column, value, nameMapping, ArithmeticRelationBuilder::isGreaterThan);
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleRelation(
          column, value, nameMapping, ArithmeticRelationBuilder::isGreaterThan);
    }
  },
  GREATER_THAN_EQUAL("gte") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return FilterOperator.buildSimpleCondition(
          column, value, nameMapping, ArithmeticRelationBuilder::isGreaterThanOrEqualTo);
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleRelation(
          column, value, nameMapping, ArithmeticRelationBuilder::isGreaterThanOrEqualTo);
    }
  },
  LESS_THAN("lt") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return FilterOperator.buildSimpleCondition(
          column, value, nameMapping, ArithmeticRelationBuilder::isLessThan);
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleRelation(column, value, nameMapping, ArithmeticRelationBuilder::isLessThan);
    }
  },
  LESS_THAN_EQUAL("lte") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return FilterOperator.buildSimpleCondition(
          column, value, nameMapping, ArithmeticRelationBuilder::isLessThanOrEqualTo);
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return buildSimpleRelation(
          column, value, nameMapping, ArithmeticRelationBuilder::isLessThanOrEqualTo);
    }
  },
  IN("in") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      return Condition.column(column.name()).in(buildListLiterals(column, value, nameMapping));
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return Relation.column(CqlIdentifier.fromInternal(column.name()))
          .in(buildListLiterals(column, value, nameMapping));
    }

    @Override
    public GraphQLInputObjectField buildField(GraphQLInputType gqlInputType) {
      return GraphQLInputObjectField.newInputObjectField()
          .name(getFieldName())
          .type(list(gqlInputType))
          .build();
    }
  },
  CONTAINS("contains") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      throw new IllegalStateException("CONTAINS can't be used on IF conditions");
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return Relation.column(CqlIdentifier.fromInternal(column.name()))
          .contains(toCqlElementTerm(column, value, nameMapping));
    }
  },
  CONTAINS_KEY("containsKey") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      throw new IllegalStateException("CONTAINS KEY can't be used on IF conditions");
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      return Relation.column(CqlIdentifier.fromInternal(column.name()))
          .containsKey(toCqlKeyTerm(column, value, nameMapping));
    }
  },
  CONTAINS_ENTRY("containsEntry") {
    @Override
    public Condition buildCondition(Column column, Object value, NameMapping nameMapping) {
      throw new IllegalStateException("CONTAINS ENTRY can't be used on IF conditions");
    }

    @Override
    public Relation buildRelation(Column column, Object value, NameMapping nameMapping) {
      Column.ColumnType mapType = column.type();
      assert mapType != null && mapType.isMap();
      Map<String, Object> entry = (Map<String, Object>) value;
      Column.ColumnType keyType = mapType.parameters().get(0);
      Term keyTerm = DataTypeMapping.toCqlTerm(keyType, entry.get("key"), nameMapping);
      Column.ColumnType valueType = mapType.parameters().get(1);
      Term valueTerm = DataTypeMapping.toCqlTerm(valueType, entry.get("value"), nameMapping);
      return Relation.mapValue(CqlIdentifier.fromInternal(column.name()), keyTerm)
          .isEqualTo(valueTerm);
    }
  },
  ;

  private static final Map<String, FilterOperator> mapByFieldName = buildMapByFieldName();
  private final String fieldName;

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

  public abstract Condition buildCondition(Column column, Object value, NameMapping nameMapping);

  private static Condition buildSimpleCondition(
      Column column,
      Object value,
      NameMapping nameMapping,
      BiFunction<ConditionBuilder<Condition>, Term, Condition> operator) {
    return operator.apply(
        Condition.column(CqlIdentifier.fromInternal(column.name())),
        DataTypeMapping.toCqlTerm(column.type(), value, nameMapping));
  }

  public abstract Relation buildRelation(Column column, Object value, NameMapping nameMapping);

  private static Relation buildSimpleRelation(
      Column column,
      Object value,
      NameMapping nameMapping,
      BiFunction<ColumnRelationBuilder<Relation>, Term, Relation> operator) {
    return operator.apply(
        Relation.column(CqlIdentifier.fromInternal(column.name())),
        DataTypeMapping.toCqlTerm(column.type(), value, nameMapping));
  }

  public GraphQLInputObjectField buildField(GraphQLInputType gqlInputType) {
    return GraphQLInputObjectField.newInputObjectField().name(fieldName).type(gqlInputType).build();
  }

  private static List<Term> buildListLiterals(Column column, Object o, NameMapping nameMapping) {
    if (o instanceof Collection<?>) {
      Collection<?> values = (Collection<?>) o;
      return values.stream()
          .map(item -> DataTypeMapping.toCqlTerm(column.type(), item, nameMapping))
          .collect(Collectors.toList());
    }

    return Collections.singletonList(DataTypeMapping.toCqlTerm(column.type(), o, nameMapping));
  }

  private static Term toCqlKeyTerm(Column column, Object value, NameMapping nameMapping) {
    Column.ColumnType mapType = column.type();
    assert mapType != null && mapType.isMap();
    Column.ColumnType keyType = mapType.parameters().get(0);
    return DataTypeMapping.toCqlTerm(keyType, value, nameMapping);
  }

  private static Term toCqlElementTerm(Column column, Object value, NameMapping nameMapping) {
    Column.ColumnType collectionType = column.type();
    assert collectionType != null && collectionType.isCollection();
    Column.ColumnType elementType = collectionType.parameters().get(collectionType.isMap() ? 1 : 0);
    return DataTypeMapping.toCqlTerm(elementType, value, nameMapping);
  }

  FilterOperator(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }
}
