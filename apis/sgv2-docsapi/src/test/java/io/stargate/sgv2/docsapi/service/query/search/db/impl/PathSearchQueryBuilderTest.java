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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class PathSearchQueryBuilderTest {

  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);

  @Inject DocumentProperties documentProperties;

  @Nested
  class BuildQuery {

    @Test
    public void simplePath() {
      List<String> path = Arrays.asList("path", "to", "something");

      PathSearchQueryBuilder builder = new PathSearchQueryBuilder(documentProperties, path);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 = ? AND p2 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("path"), Values.of("to"), Values.of("something"));
    }

    @Test
    public void glob() {
      List<String> path = Arrays.asList("path", "*", "something");

      PathSearchQueryBuilder builder = new PathSearchQueryBuilder(documentProperties, path);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 > ? AND p2 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("path"), Values.of(""), Values.of("something"));
    }

    @Test
    public void arrayGlob() {
      List<String> path = Arrays.asList("path", "[*]", "something");

      PathSearchQueryBuilder builder = new PathSearchQueryBuilder(documentProperties, path);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 > ? AND p2 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(Values.of("path"), Values.of(""), Values.of("something"));
    }

    @Test
    public void pathSplit() {
      List<String> path = Arrays.asList("path", "one,two,three", "something");

      PathSearchQueryBuilder builder = new PathSearchQueryBuilder(documentProperties, path);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 IN ? AND p2 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of(Values.of("one"), Values.of("two"), Values.of("three")),
              Values.of("something"));
    }

    @Test
    public void arrayPathSplit() {
      List<String> path = Arrays.asList("path", "[000000],[000001],[000002]", "something");

      PathSearchQueryBuilder builder = new PathSearchQueryBuilder(documentProperties, path);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\" WHERE p0 = ? AND p1 IN ? AND p2 = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of("path"),
              Values.of(Values.of("[000000]"), Values.of("[000001]"), Values.of("[000002]")),
              Values.of("something"));
    }

    @Test
    public void emptyPath() {
      List<String> path = Collections.emptyList();

      PathSearchQueryBuilder builder = new PathSearchQueryBuilder(documentProperties, path);
      QueryOuterClass.Query query =
          builder.bind(builder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME));

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM \"%s\".\"%s\"", KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
      assertThat(query.getValues().getValuesList()).isEmpty();
    }
  }
}
