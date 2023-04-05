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

package io.stargate.web.docsapi.service.write.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.web.docsapi.service.JsonShreddedRow;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class InsertQueryBuilderTest extends AbstractDataStoreTest {

  private static final int MAX_DEPTH = 8;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER =
      new DocsApiTestSchemaProvider(MAX_DEPTH);
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
  }

  @Nested
  class BuildQuery {

    @Test
    public void happyPath() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(MAX_DEPTH);

      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, null);

      String expected =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void happyPathWithTtl() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(MAX_DEPTH);

      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, 1);

      String expected =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL 1 AND TIMESTAMP ?",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }
  }

  @Nested
  class Bind {

    @Test
    public void stringValue() {
      DataStore datastore = datastore();
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String value = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("first")
              .addPath("second")
              .stringValue(value)
              .build();

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "INSERT INTO %s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              documentId,
              "first",
              "second",
              "",
              "",
              "",
              "",
              "",
              "",
              "second",
              value,
              null,
              null,
              timestamp)
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery =
          queryBuilder.bind(prepareFuture.join(), documentId, row, timestamp, false);
      datastore.execute(boundQuery);
    }

    @Test
    public void doubleValue() {
      DataStore datastore = datastore();
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      double value = RandomUtils.nextDouble();
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("first")
              .addPath("second")
              .doubleValue(value)
              .build();

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "INSERT INTO %s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              documentId,
              "first",
              "second",
              "",
              "",
              "",
              "",
              "",
              "",
              "second",
              null,
              value,
              null,
              timestamp)
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery =
          queryBuilder.bind(prepareFuture.join(), documentId, row, timestamp, false);
      datastore.execute(boundQuery);
    }

    @Test
    public void booleanValue() {
      DataStore datastore = datastore();
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      boolean value = RandomUtils.nextBoolean();
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(MAX_DEPTH)
              .addPath("first")
              .addPath("second")
              .booleanValue(value)
              .build();

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "INSERT INTO %s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              documentId,
              "first",
              "second",
              "",
              "",
              "",
              "",
              "",
              "",
              "second",
              null,
              null,
              value,
              timestamp)
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery =
          queryBuilder.bind(prepareFuture.join(), documentId, row, timestamp, false);
      datastore.execute(boundQuery);
    }

    @Test
    public void maxDepthDifferent() {
      DataStore datastore = datastore();
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME, null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(4)
              .addPath("first")
              .booleanValue(true)
              .build();

      withQuery(
          SCHEMA_PROVIDER.getTable(),
          "INSERT INTO %s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?");

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      Throwable result =
          catchThrowable(
              () -> queryBuilder.bind(prepareFuture.join(), documentId, row, timestamp, true));

      assertThat(result).isInstanceOf(IllegalArgumentException.class);
      resetExpectations();
    }
  }
}
