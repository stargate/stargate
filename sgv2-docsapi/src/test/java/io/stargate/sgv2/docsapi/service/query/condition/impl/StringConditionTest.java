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
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Literal;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.filter.operation.ValueFilterOperation;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StringConditionTest {

  @Mock ValueFilterOperation filterOperation;
  @Mock ValueFilterOperation filterOperation2;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  DocumentProperties documentProperties;

  @BeforeEach
  public void init() {
    lenient()
        .when(documentProperties.tableProperties().stringValueColumnName())
        .thenReturn("text_value");
  }

  @Nested
  class Constructor {

    @Test
    public void predicateValidated() {
      String value = RandomStringUtils.randomAlphanumeric(16);

      StringCondition condition =
          ImmutableStringCondition.of(filterOperation, value, documentProperties);

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
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.of(eq));

      ImmutableStringCondition condition =
          ImmutableStringCondition.of(filterOperation, value, documentProperties);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result)
          .hasValueSatisfying(
              builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(((Literal) builtCondition.value()).get()).isEqualTo(Values.of(value));
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("text_value"));
              });
    }

    @Test
    public void emptyPredicate() {
      String value = RandomStringUtils.randomAlphanumeric(16);
      when(filterOperation.getQueryPredicate()).thenReturn(Optional.empty());

      ImmutableStringCondition condition =
          ImmutableStringCondition.of(filterOperation, value, documentProperties);
      Optional<BuiltCondition> result = condition.getBuiltCondition();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class RowTest {

    @Mock RowWrapper row;

    @Test
    public void nullDatabaseValue() {
      String filterValue = RandomStringUtils.randomAlphanumeric(16);
      when(row.isNull("text_value")).thenReturn(true);
      when(filterOperation.test(null, filterValue)).thenReturn(true);

      ImmutableStringCondition condition =
          ImmutableStringCondition.of(filterOperation, filterValue, documentProperties);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }

    @Test
    public void notNullDatabaseValue() {
      String filterValue = RandomStringUtils.randomAlphanumeric(16);
      String databaseValue = RandomStringUtils.randomAlphanumeric(16);
      when(row.isNull("text_value")).thenReturn(false);
      when(row.getString("text_value")).thenReturn(databaseValue);
      when(filterOperation.test(databaseValue, filterValue)).thenReturn(true);

      ImmutableStringCondition condition =
          ImmutableStringCondition.of(filterOperation, filterValue, documentProperties);
      boolean result = condition.test(row);

      assertThat(result).isTrue();
    }
  }

  @Nested
  class Negation {
    @Test
    void simple() {
      when(filterOperation.negate()).thenReturn(filterOperation2);

      StringCondition condition =
          ImmutableStringCondition.of(filterOperation, "test123", documentProperties);

      assertThat(condition.negate())
          .isInstanceOfSatisfying(
              StringCondition.class,
              negated -> {
                assertThat(negated.getQueryValue()).isEqualTo("test123");
                assertThat(negated.getFilterOperation()).isEqualTo(filterOperation2);
                assertThat(negated.documentProperties()).isEqualTo(documentProperties);
              });
    }
  }
}
