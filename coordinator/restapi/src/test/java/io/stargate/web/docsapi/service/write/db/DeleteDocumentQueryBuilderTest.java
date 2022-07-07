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

import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DeleteDocumentQueryBuilderTest extends AbstractDataStoreTest {

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
      DeleteDocumentQueryBuilder queryBuilder = new DeleteDocumentQueryBuilder();

      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ?", KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }
  }

  @Nested
  class Bind {

    @Test
    public void happyPath() {
      DataStore datastore = datastore();
      DeleteDocumentQueryBuilder queryBuilder = new DeleteDocumentQueryBuilder();
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);

      withQuery(
              SCHEMA_PROVIDER.getTable(),
              "DELETE FROM %s USING TIMESTAMP ? WHERE key = ?",
              new Object[] {timestamp, documentId})
          .returningNothing();

      CompletableFuture<? extends Query<? extends BoundQuery>> prepareFuture =
          datastore.prepare(query);
      BoundQuery boundQuery = queryBuilder.bind(prepareFuture.join(), documentId, timestamp);
      datastore.execute(boundQuery);
    }
  }
}
