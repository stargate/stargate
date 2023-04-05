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
import static org.mockito.Mockito.when;

import io.stargate.db.datastore.AbstractDataStoreTest;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.query.FilterExpression;
import io.stargate.web.docsapi.service.query.FilterPath;
import io.stargate.web.docsapi.service.query.ImmutableFilterPath;
import io.stargate.web.docsapi.service.query.condition.BaseCondition;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DocumentsSearchQueryBuilderTest extends AbstractDataStoreTest {

  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER = new DocsApiTestSchemaProvider(4);
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().name();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().name();

  @Mock DocsApiConfiguration config;

  @Mock FilterExpression filterExpression;

  @Override
  protected Schema schema() {
    return SCHEMA_PROVIDER.getSchema();
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
                  new DocumentSearchQueryBuilder(
                      Arrays.asList(filterExpression, filterExpression), config));

      assertThat(t).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  class BuildQuery {

    @Mock BaseCondition condition;

    @Test
    public void happyPath() {
      when(config.getMaxDepth()).thenReturn(64);
      FilterPath filterPath = ImmutableFilterPath.of(Collections.singletonList("field"));
      when(filterExpression.getFilterPath()).thenReturn(filterPath);
      when(filterExpression.getCondition()).thenReturn(condition);
      when(condition.getBuiltCondition()).thenReturn(Optional.empty());

      FilterExpressionSearchQueryBuilder builder =
          new DocumentSearchQueryBuilder(filterExpression, config);
      BuiltQuery<? extends BoundQuery> query =
          builder.buildQuery(datastore()::queryBuilder, KEYSPACE_NAME, COLLECTION_NAME);

      String expected =
          String.format(
              "SELECT WRITETIME(leaf) FROM %s.%s WHERE p0 = 'field' AND leaf = 'field' AND p1 = '' AND key = ? ALLOW FILTERING",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.toString()).isEqualTo(expected);
    }
  }
}
