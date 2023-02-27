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

import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FullSearchQueryBuilderTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(0);
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
      FullSearchQueryBuilder queryBuilder = new FullSearchQueryBuilder();
      BuiltQuery<? extends BoundQuery> query =
          queryBuilder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format("SELECT WRITETIME(leaf) FROM %s.%s", KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }
  }
}
