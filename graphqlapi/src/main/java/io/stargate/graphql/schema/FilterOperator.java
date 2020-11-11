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
package io.stargate.graphql.schema;

import static graphql.schema.GraphQLList.list;

import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputType;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** Represents the relational and equality operators for filtering in GraphQL queries. */
public enum FilterOperator {
  EQUAL("eq") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      return Condition.column(CqlIdentifier.fromInternal(columnName)).isEqualTo(value);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      return relationStart.isEqualTo(value);
    }
  },
  NOT_EQUAL("notEq") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      return Condition.column(columnName).isNotEqualTo(value);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      return relationStart.isNotEqualTo(value);
    }
  },
  GREATER_THAN("gt") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      return Condition.column(columnName).isGreaterThan(value);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      return relationStart.isGreaterThan(value);
    }
  },
  GREATER_THAN_EQUAL("gte") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      return Condition.column(columnName).isGreaterThanOrEqualTo(value);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      return relationStart.isGreaterThanOrEqualTo(value);
    }
  },
  LESS_THAN("lt") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      return Condition.column(columnName).isLessThan(value);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      return relationStart.isLessThan(value);
    }
  },
  LESS_THAN_EQUAL("lte") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      return Condition.column(columnName).isLessThanOrEqualTo(value);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      return relationStart.isLessThanOrEqualTo(value);
    }
  },
  IN("in") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      throw new IllegalStateException("Single item call is not valid for IN operator");
    }

    @Override
    public Condition buildCondition(String columnName, Iterable<Term> values) {
      return Condition.column(columnName).in(values);
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      throw new IllegalStateException("Single item call is not valid for IN operator");
    }

    @Override
    public Relation buildRelation(String columnName, Iterable<Term> values) {
      return Relation.column(columnName).in(values);
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
    public Condition buildCondition(String columnName, Term value) {
      throw new IllegalStateException("CONTAINS can't be used on IF conditions");
    }

    @Override
    public Relation buildRelation(
        ColumnRelationBuilder<Relation> relationStart, Term elementValue) {
      return relationStart.contains(elementValue);
    }
  },
  CONTAINS_KEY("containsKey") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      throw new IllegalStateException("CONTAINS KEY can't be used on IF conditions");
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term key) {
      return relationStart.containsKey(key);
    }
  },
  CONTAINS_ENTRY("containsEntry") {
    @Override
    public Condition buildCondition(String columnName, Term value) {
      throw new IllegalStateException("CONTAINS ENTRY can't be used on IF conditions");
    }

    @Override
    public Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value) {
      throw new IllegalStateException("CONTAINS ENTRY requires key and value terms");
    }

    @Override
    public Relation buildRelation(String columnName, Term key, Term value) {
      return Relation.mapValue(columnName, key).isEqualTo(value);
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

  public abstract Condition buildCondition(String columnName, Term value);

  public Condition buildCondition(String columnName, Iterable<Term> values) {
    throw new IllegalStateException(
        "Can't build condition from multiple values for " + this.toString());
  }

  public Relation buildRelation(String columnName, Term value) {
    return buildRelation(Relation.column(columnName), value);
  }

  public abstract Relation buildRelation(ColumnRelationBuilder<Relation> relationStart, Term value);

  public Relation buildRelation(String columnName, Iterable<Term> values) {
    throw new IllegalStateException(
        "Can't build relation from multiple values for " + this.toString());
  }

  public Relation buildRelation(String columnName, Term key, Term value) {
    throw new IllegalStateException(
        "Can't build relation from key and value for " + this.toString());
  }

  public GraphQLInputObjectField buildField(GraphQLInputType gqlInputType) {
    return GraphQLInputObjectField.newInputObjectField().name(fieldName).type(gqlInputType).build();
  }

  FilterOperator(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }
}
