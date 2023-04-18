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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.service.query.search.db.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.Term;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.query.FilterExpression;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.service.query.condition.BaseCondition;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class FilterExpressionSearchQueryBuilderTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Mock FilterExpression filterExpression;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  @Nested
  class Constructor {

    @Test
    public void differentPathsNotAllowed() {
      FilterPath filterPath1 = ImmutableFilterPath.of(Collections.singletonList("field"));
      FilterPath filterPath2 = ImmutableFilterPath.of(Collections.singletonList("another"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath1).thenReturn(filterPath2);

      Throwable t =
          catchThrowable(
              () ->
                  new FilterExpressionSearchQueryBuilder(
                      documentProperties, Arrays.asList(filterExpression, filterExpression)));

      assertThat(t).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class BuildQuery {

    @Mock BaseCondition condition;

    FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));

    @BeforeEach
    public void initMocks() {
      MockitoAnnotations.openMocks(this);
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
    }

    @Test
    public void happyPath() {
      QueryOuterClass.Value textValue = Values.of("value");
      BuiltCondition builtCondition = BuiltCondition.of("text_value", Predicate.EQ, Term.marker());
      when(condition.getBuiltCondition())
          .thenReturn(Optional.of(Pair.of(builtCondition, textValue)));

      FilterExpressionSearchQueryBuilder builder =
          new FilterExpressionSearchQueryBuilder(documentProperties, filterExpression);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? AND text_value = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("field"), Values.of("field"), Values.of(""), textValue);
    }

    @Test
    public void noCondition() {
      when(condition.getBuiltCondition()).thenReturn(Optional.empty());

      FilterExpressionSearchQueryBuilder builder =
          new FilterExpressionSearchQueryBuilder(documentProperties, filterExpression);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("field"), Values.of("field"), Values.of(""));
    }
  }
}
