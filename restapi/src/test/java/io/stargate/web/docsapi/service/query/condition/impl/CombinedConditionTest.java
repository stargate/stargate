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
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.filter.operation.CombinedFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.ExistsFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.web.docsapi.service.query.filter.operation.impl.NotInFilterOperation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
    public void allMatchAllFalse() {
      Object filterValue = new Object();
      when(filterOperation.isMatchAll()).thenReturn(true);

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void allMatchOneFalse() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getBoolean("bool_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn("Jordan");
      when(row.getDouble("dbl_value")).thenReturn(23d);
      when(filterOperation.isMatchAll()).thenReturn(true);
      when(filterOperation.test(eq(filterValue), eq(false))).thenReturn(true);
      when(filterOperation.test(eq(filterValue), eq("Jordan"))).thenReturn(false);
      when(filterOperation.test(eq(filterValue), eq(23d))).thenReturn(true);

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void allMatchAllTrue() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getBoolean("bool_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn("Jordan");
      when(row.getDouble("dbl_value")).thenReturn(23d);
      when(filterOperation.isMatchAll()).thenReturn(true);
      when(filterOperation.test(eq(filterValue), eq(false))).thenReturn(true);
      when(filterOperation.test(eq(filterValue), eq("Jordan"))).thenReturn(true);
      when(filterOperation.test(eq(filterValue), eq(23d))).thenReturn(true);

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void anyMatchAllFalse() {
      Object filterValue = new Object();
      when(filterOperation.isMatchAll()).thenReturn(false);

      CombinedCondition<Object> condition =
          ImmutableCombinedCondition.of(filterOperation, filterValue, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void anyMatchOneTrue() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(false);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.isNull("dbl_value")).thenReturn(false);
      when(row.getBoolean("bool_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn("Jordan");
      when(row.getDouble("dbl_value")).thenReturn(23d);
      when(filterOperation.isMatchAll()).thenReturn(false);
      when(filterOperation.test(eq(filterValue), eq(false))).thenReturn(false);
      when(filterOperation.test(eq(filterValue), eq("Jordan"))).thenReturn(true);
      when(filterOperation.test(eq(filterValue), eq(23d))).thenReturn(false);

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
    public void existsPredicateNotMatched() {
      CombinedCondition<Boolean> condition =
          ImmutableCombinedCondition.of(ExistsFilterOperation.of(), true, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void existsPredicateMatch() {
      String findMe = "find-me";
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(findMe);

      CombinedCondition<Boolean> condition =
          ImmutableCombinedCondition.of(ExistsFilterOperation.of(), true, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
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
