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
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubDocumentSearchQueryBuilderTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
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
      List<String> path = Arrays.asList("path", "to", "something");
      String docId = "d1234";

      SubDocumentSearchQueryBuilder builder = new SubDocumentSearchQueryBuilder(docId, path);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'path' AND p1 = 'to' AND p2 = 'something' AND key = 'd1234' ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }
  }
}
