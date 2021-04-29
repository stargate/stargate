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

package io.stargate.web.docsapi.service.query.condition.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.filter.operation.DoubleValueFilterOperation;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NumberConditionTest {

  @Mock DoubleValueFilterOperation<Number> filterOperation;

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {
      Number value = RandomUtils.nextLong();

      NumberCondition condition = ImmutableNumberCondition.of(filterOperation, value);

      assertThat(condition).isNotNull();
      verify(filterOperation).validateDoubleFilterInput(value);
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

      ImmutableNumberCondition condition = ImmutableNumberCondition.of(filterOperation, value);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(builtCondition.value().get()).isEqualTo(value.doubleValue());
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("dbl_value"));
              });
    }

    @Test
    public void emptyPredicate() {
      Number value = RandomUtils.nextLong();
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.empty());

      ImmutableNumberCondition condition = ImmutableNumberCondition.of(filterOperation, value);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock Row row;

    @Test
    public void nullDatabaseValue() {
      Number filterValue = RandomUtils.nextLong();
      when(row.isNull("dbl_value")).thenReturn(true);
      when(filterOperation.test(filterValue, null)).thenReturn(true);

      ImmutableNumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, filterValue);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notNullDatabaseValue() {
      Number filterValue = RandomUtils.nextLong();
      Double databaseValue = RandomUtils.nextDouble();
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getDouble("dbl_value")).thenReturn(databaseValue);
      when(filterOperation.test(filterValue, databaseValue)).thenReturn(true);

      ImmutableNumberCondition condition =
          ImmutableNumberCondition.of(filterOperation, filterValue);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }
}
