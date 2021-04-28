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

import io.stargate.db.datastore.Row;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.filter.operation.CombinedFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NotInFilterOperation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CombinedConditionTest {

  @Mock CombinedFilterOperation<Object> filterOperation;

  @Mock Row row;

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {
      Object queryValue = new Object();

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, queryValue, true);

      assertThat(condition).isNotNull();
      verify(filterOperation).validateBooleanFilterInput(queryValue);
      verify(filterOperation).validateStringFilterInput(queryValue);
      verify(filterOperation).validateDoubleFilterInput(queryValue);
      verifyNoMoreInteractions(filterOperation);
    }
  }

  @Nested
  class GetBuiltCondition {

    @Test
    public void alwaysEmpty() {

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, new Object(), true);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock Row row;

    @Test
    public void emptyRow() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(true);
      when(row.isNull("text_value")).thenReturn(true);
      when(row.isNull("dbl_value")).thenReturn(true);
      when(filterOperation.test(filterValue, (String) null)).thenReturn(true);

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanValue() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.isNull("text_value")).thenReturn(true);
      when(row.isNull("dbl_value")).thenReturn(true);
      when(row.getBoolean("bool_value")).thenReturn(false);
      when(filterOperation.test(filterValue, false)).thenReturn(true);

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanNumericValue() {
      Object filterValue = new Object();
      Byte byteValue = 0;
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.isNull("text_value")).thenReturn(true);
      when(row.isNull("dbl_value")).thenReturn(true);
      when(row.getByte("bool_value")).thenReturn(byteValue);
      when(filterOperation.test(filterValue, false)).thenReturn(true);

      CombinedCondition<Object> condition =
              ImmutableCombinedCondition.of(filterOperation, filterValue, true);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void numberValue() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(true);
      when(row.isNull("text_value")).thenReturn(true);
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getDouble("dbl_value")).thenReturn(22d);
      when(filterOperation.test(filterValue, 22d)).thenReturn(true);

      CombinedCondition<Object> condition =
              ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void stringValue() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(true);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.isNull("dbl_value")).thenReturn(true);
      when(row.getString("text_value")).thenReturn("Jordan");
      when(filterOperation.test(filterValue, "Jordan")).thenReturn(true);

      CombinedCondition<Object> condition =
              ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

  }

  // set of simple int test in order to confirm with existing predicates
  @Nested
  class Integration {

    @BeforeEach
    public void initRow() {
      when(row.isNull("bool_value")).thenReturn(true);
      when(row.isNull("dbl_value")).thenReturn(true);
      when(row.isNull("text_value")).thenReturn(true);
    }

    @Test
    public void inPredicateNotMatched() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);

      CombinedCondition<List<?>> condition =
          ImmutableCombinedCondition.of(InFilterOperation.of(), queryValue, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void inPredicateMatch() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(findMe);

      CombinedCondition<List<?>> condition =
          ImmutableCombinedCondition.of(InFilterOperation.of(), queryValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notInPredicateEmptyRow() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);

      CombinedCondition<List<?>> condition =
          ImmutableCombinedCondition.of(NotInFilterOperation.of(), queryValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notInPredicateNotMatch() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(findMe);

      CombinedCondition<List<?>> condition =
          ImmutableCombinedCondition.of(NotInFilterOperation.of(), queryValue, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void notInPredicateMatch() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn("something");

      CombinedCondition<List<?>> condition =
          ImmutableCombinedCondition.of(NotInFilterOperation.of(), queryValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }
}
