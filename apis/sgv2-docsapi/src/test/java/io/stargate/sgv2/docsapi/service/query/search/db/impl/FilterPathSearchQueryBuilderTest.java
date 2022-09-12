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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.query.FilterPath;
import io.stargate.sgv2.docsapi.service.query.ImmutableFilterPath;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class FilterPathSearchQueryBuilderTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Nested
  class BuildQuery {

    @Test
    public void fieldOnly() {
      List<String> path = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
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

    @Test
    public void fieldWithColumns() {
      List<String> path = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(
              builder.buildQuery(
                  KEYSPACE_NAME,
                  COLLECTION_NAME,
                  documentProperties.tableProperties().pathColumnName(0)));

      String expected =
          String.format(
              "SELECT p0, WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("field"), Values.of("field"), Values.of(""));
    }

    @Test
    public void fieldWithLimit() {
      List<String> path = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, 5));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND leaf = ? AND p1 = ? LIMIT 5 ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("field"), Values.of("field"), Values.of(""));
    }

    @Test
    public void fieldAndParentPath() {
      List<String> path = Arrays.asList("path", "to", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of("to"),
              Values.of("field"),
              Values.of("field"),
              Values.of(""));
    }

    @Test
    public void fieldAndParentGlob() {
      List<String> path = Arrays.asList("path", "*", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 > ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of(""),
              Values.of("field"),
              Values.of("field"),
              Values.of(""));
    }

    @Test
    public void fieldAndParentArrayGlob() {
      List<String> path = Arrays.asList("path", "[*]", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 > ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of(""),
              Values.of("field"),
              Values.of("field"),
              Values.of(""));
    }

    @Test
    public void fieldAndParentPathSplit() {
      List<String> path = Arrays.asList("path", "one,two,three", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 IN ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of(Values.of("one"), Values.of("two"), Values.of("three")),
              Values.of("field"),
              Values.of("field"),
              Values.of(""));
    }

    @Test
    public void fieldAndParentArrayPathSplit() {
      List<String> path = Arrays.asList("path", "[000000],[000001],[000002]", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 IN ? AND p2 = ? AND leaf = ? AND p3 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of(Values.of("[000000]"), Values.of("[000001]"), Values.of("[000002]")),
              Values.of("field"),
              Values.of("field"),
              Values.of(""));
    }

    @Test
    public void maxDepthExceeded() {
      List<String> path =
          IntStream.range(0, documentProperties.maxDepth() + 1)
              .mapToObj(Integer::toString)
              .collect(Collectors.toList());
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(documentProperties, filterPath, true);
      Throwable t = catchThrowable(() -> builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }
  }
}
