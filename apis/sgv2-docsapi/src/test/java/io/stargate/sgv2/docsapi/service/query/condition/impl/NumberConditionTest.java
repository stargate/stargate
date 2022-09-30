/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.sgv2.docsapi.service.query.condition.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Marker;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.filter.operation.ValueFilterOperation;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NumberConditionTest {

  @Mock ValueFilterOperation filterOperation;
  @Mock ValueFilterOperation filterOperation2;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  DocumentProperties documentProperties;

  @BeforeEach
  public void init() {
    lenient()
        .when(documentProperties.tableProperties().doubleValueColumnName())
        .thenReturn("dbl_value");
  }

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {
      Number value = RandomUtils.nextLong();

      NumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, value, documentProperties);

      assertThat(condition).isNotNull();
      verify(filterOperation).validateNumberFilterInput(value);
      verifyNoMoreInteractions(filterOperation);
    }
  }

  @Nested
  class GetBuiltCondition {

    @Test
    public void happyPath() {
      Predicate eq = Predicate.EQ;
      Number value = RandomUtils.nextLong();
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.of(eq));

      ImmutableNumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, value, documentProperties);
      Optional<Pair<BuiltCondition, QueryOuterClass.Value>> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.getLeft().predicate()).isEqualTo(eq);
                assertThat(builtCondition.getLeft().value()).isInstanceOf(Marker.class);
                assertThat(builtCondition.getLeft().lhs())
                    .isEqualTo(BuiltCondition.LHS.column("dbl_value"));
                assertThat((builtCondition.getRight())).isEqualTo(Values.of(value.doubleValue()));
              });
    }

    @Test
    public void emptyPredicate() {
      Number value = RandomUtils.nextLong();
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.empty());

      ImmutableNumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, value, documentProperties);
      Optional<Pair<BuiltCondition, QueryOuterClass.Value>> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock RowWrapper row;

    @Test
    public void nullDatabaseValue() {
      Number filterValue = RandomUtils.nextLong();
      when(row.isNull("dbl_value")).thenReturn(true);
      when(filterOperation.test(null, filterValue)).thenReturn(true);

      ImmutableNumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, filterValue, documentProperties);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notNullDatabaseValue() {
      Number filterValue = RandomUtils.nextLong();
      Double databaseValue = RandomUtils.nextDouble();
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getDouble("dbl_value")).thenReturn(databaseValue);
      when(filterOperation.test(databaseValue, filterValue)).thenReturn(true);

      ImmutableNumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, filterValue, documentProperties);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }

  @Nested
  class Negation {
    @Test
    void simple() {
      when(filterOperation.negate()).thenReturn(filterOperation2);

      NumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, 123, documentProperties);

      assertThat(condition.negate())
          .isInstanceOfSatisfying(
              NumberCondition.class,
              negated -> {
                assertThat(negated.getQueryValue()).isEqualTo(123);
                assertThat(negated.getFilterOperation()).isEqualTo(filterOperation2);
                assertThat(negated.documentProperties()).isEqualTo(documentProperties);
              });
    }
  }
}
