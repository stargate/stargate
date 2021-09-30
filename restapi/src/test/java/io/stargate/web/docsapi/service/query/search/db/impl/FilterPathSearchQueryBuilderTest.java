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

package io.stargate.web.docsapi.service.query.search.db.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FilterPathSearchQueryBuilderTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Mock DocsApiConfiguration config;

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @Nested
  class BuildQuery {

    @Test
    public void fieldOnly() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'field' AND leaf = 'field' AND p1 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldWithColumns() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(
              datastore()::queryBuilder,
              KEYSPACE_NAME,
              COLLECTION_NAME,
              DocsApiConstants.P_COLUMN_NAME.apply(0));

      String expected =
          String.format(
              "SELECT p0, WRITETIME(leaf) FROM %s.%s WHERE p0 = 'field' AND leaf = 'field' AND p1 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldWithLimit() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Collections.singletonList("field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, 5);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'field' AND leaf = 'field' AND p1 = '' LIMIT 5 ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldAndParentPath() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Arrays.asList("path", "to", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'path' AND p1 = 'to' AND p2 = 'field' AND leaf = 'field' AND p3 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldAndParentGlob() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Arrays.asList("path", "*", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'path' AND p1 > '' AND p2 = 'field' AND leaf = 'field' AND p3 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldAndParentArrayGlob() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Arrays.asList("path", "[*]", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'path' AND p1 > '' AND p2 = 'field' AND leaf = 'field' AND p3 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldAndParentPathSplit() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Arrays.asList("path", "one,two,three", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'path' AND p1 IN ('one', 'two', 'three') AND p2 = 'field' AND leaf = 'field' AND p3 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void fieldAndParentArrayPathSplit() {
      Mockito.when(config.getMaxDepth()).thenReturn(64);
      List<String> path = Arrays.asList("path", "[000000],[000001],[000002]", "field");
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'path' AND p1 IN ('[000000]', '[000001]', '[000002]') AND p2 = 'field' AND leaf = 'field' AND p3 = '' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void maxDepthExceeded() {
      List<String> path =
          IntStream.range(0, config.getMaxDepth() + 1)
              .mapToObj(Integer::toString)
              .collect(Collectors.toList());
      FilterPath filterPath = ImmutableFilterPath.of(path);

      FilterPathSearchQueryBuilder builder =
          new FilterPathSearchQueryBuilder(filterPath, true, config);
      Throwable t =
          catchThrowable(
              () -> builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME));

      assertThat(t)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }
  }
}
