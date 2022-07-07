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
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DeleteSubDocumentPathQueryBuilderTest extends AbstractDataStoreTest {

  private static final int MAX_DEPTH = 4;
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
      List<String> subDocumentPath = Arrays.asList("one", "two");
      DeleteSubDocumentPathQueryBuilder queryBuilder =
          new DeleteSubDocumentPathQueryBuilder(subDocumentPath, false, MAX_DEPTH);

      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ?",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void happyPathExact() {
      List<String> subDocumentPath = Arrays.asList("one", "two");
      DeleteSubDocumentPathQueryBuilder queryBuilder =
          new DeleteSubDocumentPathQueryBuilder(subDocumentPath, true, MAX_DEPTH);

      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }

    @Test
    public void maxDepthExceeded() {
      List<String> subDocumentPath = Arrays.asList("1", "2", "3", "4", "5");
      DeleteSubDocumentPathQueryBuilder queryBuilder =
          new DeleteSubDocumentPathQueryBuilder(subDocumentPath, false, MAX_DEPTH);

      Throwable throwable =
          catchThrowable(
              () ->
                  queryBuilder.buildQuery(
                      datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_GENERAL_DEPTH_EXCEEDED);
    }
  }

  @Nested
  class Bind {

    @Test
    public void happyPath() {
      DataStore datastore = datastore();
      List<String> subDocumentPath = Arrays.asList("one", "two");
      DeleteSubDocumentPathQueryBuilder queryBuilder =
          new DeleteSubDocumentPathQueryBuilder(subDocumentPath, false, MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ?",
              new Object[] {timestamp, documentId, "one", "two"})
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery = queryBuilder.bind(prepareFuture.join(), documentId, timestamp);
      datastore.execute(boundQuery);
    }

    @Test
    public void happyPathExact() {
      DataStore datastore = datastore();
      List<String> subDocumentPath = Arrays.asList("one", "two");
      DeleteSubDocumentPathQueryBuilder queryBuilder =
          new DeleteSubDocumentPathQueryBuilder(subDocumentPath, true, MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?",
              new Object[] {timestamp, documentId, "one", "two", "", ""})
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery = queryBuilder.bind(prepareFuture.join(), documentId, timestamp);
      datastore.execute(boundQuery);
    }

    @Test
    public void happyPathFullExact() {
      DataStore datastore = datastore();
      List<String> subDocumentPath = Arrays.asList("one", "two", "three", "four");
      DeleteSubDocumentPathQueryBuilder queryBuilder =
          new DeleteSubDocumentPathQueryBuilder(subDocumentPath, true, MAX_DEPTH);
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "DELETE FROM %s USING TIMESTAMP ? WHERE key = ? AND p0 = ? AND p1 = ? AND p2 = ? AND p3 = ?",
              new Object[] {timestamp, documentId, "one", "two", "three", "four"})
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery = queryBuilder.bind(prepareFuture.join(), documentId, timestamp);
      datastore.execute(boundQuery);
    }
  }
}
