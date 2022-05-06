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

import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.filter.operation.GenericFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.InFilterOperation;
import io.stargate.sgv2.docsapi.service.query.filter.operation.impl.NotInFilterOperation;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GenericConditionTest {

  @Mock GenericFilterOperation<Object> filterOperation;
  @Mock GenericFilterOperation<Object> filterOperation2;
  @Mock RowWrapper row;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  DocumentProperties documentProperties;

  @BeforeEach
  public void init() {
    lenient()
        .when(documentProperties.tableProperties().stringValueColumnName())
        .thenReturn("text_value");
    lenient()
        .when(documentProperties.tableProperties().doubleValueColumnName())
        .thenReturn("dbl_value");
    lenient()
        .when(documentProperties.tableProperties().booleanValueColumnName())
        .thenReturn("bool_value");
  }

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {
      Object queryValue = new Object();

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, queryValue, documentProperties, true);

      assertThat(condition).isNotNull();
      verify(filterOperation).validateFilterInput(queryValue);
      verifyNoMoreInteractions(filterOperation);
    }
  }

  @Nested
  class GetBuiltCondition {

    @Test
    public void alwaysEmpty() {

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, new Object(), documentProperties, true);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock RowWrapper row;

    @Test
    public void emptyRow() {
      Object filterValue = new Object();
      when(row.isNull("bool_value")).thenReturn(true);
      when(row.isNull("text_value")).thenReturn(true);
      when(row.isNull("dbl_value")).thenReturn(true);
      when(filterOperation.test((String) null, filterValue)).thenReturn(true);

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, filterValue, documentProperties, false);
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
      when(filterOperation.test(false, filterValue)).thenReturn(true);

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, filterValue, documentProperties, false);
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
      when(filterOperation.test(false, filterValue)).thenReturn(true);

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, filterValue, documentProperties, true);
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
      when(filterOperation.test(22d, filterValue)).thenReturn(true);

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, filterValue, documentProperties, false);
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
      when(filterOperation.test("Jordan", filterValue)).thenReturn(true);

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, filterValue, documentProperties, false);
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

      GenericCondition<List<?>> condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), queryValue, documentProperties, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void inPredicateMatch() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(findMe);

      GenericCondition<List<?>> condition =
          ImmutableGenericCondition.of(
              InFilterOperation.of(), queryValue, documentProperties, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notInPredicateEmptyRow() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);

      GenericCondition<List<?>> condition =
          ImmutableGenericCondition.of(
              NotInFilterOperation.of(), queryValue, documentProperties, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notInPredicateNotMatch() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(findMe);

      GenericCondition<List<?>> condition =
          ImmutableGenericCondition.of(
              NotInFilterOperation.of(), queryValue, documentProperties, false);
      boolean result = condition.test(row);

      assertThat(result).isFalse();
    }

    @Test
    public void notInPredicateMatch() {
      String findMe = "find-me";
      List<?> queryValue = Collections.singletonList(findMe);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn("something");

      GenericCondition<List<?>> condition =
          ImmutableGenericCondition.of(
              NotInFilterOperation.of(), queryValue, documentProperties, false);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }

  @Nested
  class Negation {
    @Test
    void simple() {
      List<?> queryValue = Collections.singletonList("test");
      when(filterOperation.negate()).thenReturn(filterOperation2);

      GenericCondition<Object> condition =
          ImmutableGenericCondition.of(filterOperation, queryValue, documentProperties, true);

      assertThat(condition.negate())
          .isInstanceOfSatisfying(
              GenericCondition.class,
              negated -> {
                assertThat(negated.getQueryValue()).isEqualTo(queryValue);
                assertThat(negated.getFilterOperation()).isEqualTo(filterOperation2);
                assertThat(negated.isNumericBooleans()).isEqualTo(true);
              });
    }
  }
}
