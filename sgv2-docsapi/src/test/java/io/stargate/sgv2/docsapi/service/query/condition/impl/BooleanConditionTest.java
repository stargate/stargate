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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Literal;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.service.query.filter.operation.ValueFilterOperation;
import io.stargate.sgv2.docsapi.service.util.ExtendedRow;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BooleanConditionTest {

  @Mock ValueFilterOperation filterOperation;
  @Mock ValueFilterOperation filterOperation2;

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {

      BooleanCondition condition = ImmutableBooleanCondition.of(filterOperation, true, false);

      assertThat(condition).isNotNull();
      verify(filterOperation).validateBooleanFilterInput(true);
      verifyNoMoreInteractions(filterOperation);
    }
  }

  @Nested
  class GetBuiltCondition {

    @Test
    public void happyPath() {
      Predicate eq = Predicate.EQ;
      boolean value = RandomUtils.nextBoolean();
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.of(eq));

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, value, false);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(((Literal) builtCondition.value()).get()).isEqualTo(Values.of(value));
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("bool_value"));
              });
    }

    @Test
    public void numericConditionsTrue() {
      Predicate eq = Predicate.EQ;
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.of(eq));

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, true, true);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(((Literal) builtCondition.value()).get()).isEqualTo(Values.of(1));
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("bool_value"));
              });
    }

    @Test
    public void numericConditionsFalse() {
      Predicate eq = Predicate.EQ;
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.of(eq));

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, false, true);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(((Literal) builtCondition.value()).get()).isEqualTo(Values.of(0));
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("bool_value"));
              });
    }

    @Test
    public void emptyPredicate() {
      boolean value = RandomUtils.nextBoolean();
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.empty());

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, value, true);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock ExtendedRow row;

    @Test
    public void nullDatabaseValue() {
      boolean filterValue = RandomUtils.nextBoolean();
      when(row.isNull("bool_value")).thenReturn(true);
      when(filterOperation.test(null, filterValue)).thenReturn(true);

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void nonNumericBooleans() {
      boolean filterValue = RandomUtils.nextBoolean();
      boolean databaseValue = RandomUtils.nextBoolean();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getBoolean("bool_value")).thenReturn(databaseValue);
      when(filterOperation.test(databaseValue, filterValue)).thenReturn(true);

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void numericBooleansZero() {
      boolean filterValue = RandomUtils.nextBoolean();
      byte databaseValue = 0;
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getByte("bool_value")).thenReturn(databaseValue);
      when(filterOperation.test(false, filterValue)).thenReturn(true);

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, filterValue, true);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void numericBooleansOne() {
      boolean filterValue = RandomUtils.nextBoolean();
      byte databaseValue = 1;
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.getByte("bool_value")).thenReturn(databaseValue);
      when(filterOperation.test(true, filterValue)).thenReturn(true);

      ImmutableBooleanCondition condition =
          ImmutableBooleanCondition.of(filterOperation, filterValue, true);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }

  @Nested
  class Negation {
    @ParameterizedTest
    @CsvSource({"true", "false"})
    void simple(boolean queryValue) {
      when(filterOperation.negate()).thenReturn(filterOperation2);

      BooleanCondition condition = ImmutableBooleanCondition.of(filterOperation, queryValue, true);

      assertThat(condition.negate())
          .isInstanceOfSatisfying(
              BooleanCondition.class,
              negated -> {
                assertThat(negated.getQueryValue()).isEqualTo(queryValue);
                assertThat(negated.getFilterOperation()).isEqualTo(filterOperation2);
                assertThat(negated.isNumericBooleans()).isEqualTo(true);
              });
    }
  }
}
