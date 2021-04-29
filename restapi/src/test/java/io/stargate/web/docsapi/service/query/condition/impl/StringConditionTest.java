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
import io.stargate.web.docsapi.service.query.filter.operation.StringValueFilterOperation;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StringConditionTest {

  @Mock StringValueFilterOperation<String> filterOperation;

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {
      String value = RandomStringUtils.randomAlphanumeric(16);

      StringCondition condition = ImmutableStringCondition.of(filterOperation, value);

      assertThat(condition).isNotNull();
      verify(filterOperation).validateStringFilterInput(value);
      verifyNoMoreInteractions(filterOperation);
    }
  }

  @Nested
  class GetBuiltCondition {

    @Test
    public void happyPath() {
      Predicate eq = Predicate.EQ;
      String value = RandomStringUtils.randomAlphanumeric(16);
      when(filterOperation.getDatabasePredicate()).thenReturn(Optional.of(eq));

      ImmutableStringCondition condition = ImmutableStringCondition.of(filterOperation, value);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(builtCondition.value().get()).isEqualTo(value);
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("text_value"));
              });
    }

    @Test
    public void emptyPredicate() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      when(filterOperation.getDatabasePredicate()).thenReturn(Optional.empty());

      ImmutableStringCondition condition = ImmutableStringCondition.of(filterOperation, value);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock Row row;

    @Test
    public void nullDatabaseValue() {
      String filterValue = RandomStringUtils.randomAlphanumeric(16);
      when(row.isNull("text_value")).thenReturn(true);
      when(filterOperation.test(filterValue, null)).thenReturn(true);

      ImmutableStringCondition condition =
          ImmutableStringCondition.of(filterOperation, filterValue);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notNullDatabaseValue() {
      String filterValue = RandomStringUtils.randomAlphanumeric(16);
      String databaseValue = RandomStringUtils.randomAlphanumeric(16);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(databaseValue);
      when(filterOperation.test(filterValue, databaseValue)).thenReturn(true);

      ImmutableStringCondition condition =
          ImmutableStringCondition.of(filterOperation, filterValue);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }
}
