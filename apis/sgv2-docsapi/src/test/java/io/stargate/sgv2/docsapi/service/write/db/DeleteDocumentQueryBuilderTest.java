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

package io.stargate.sgv2.docsapi.service.write.db;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth4TestProfile;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth4TestProfile.class)
class DeleteDocumentQueryBuilderTest {

  @Inject DocumentProperties documentProperties;
  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Nested
  class BuildQuery {

    @Test
    public void happyPath() {
      DeleteDocumentQueryBuilder queryBuilder = new DeleteDocumentQueryBuilder(documentProperties);

      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName());

      String expected =
          String.format(
              "DELETE FROM %s.%s USING TIMESTAMP ? WHERE key = ?",
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName());
      assertThat(query.getCql()).isEqualTo(expected);
    }
  }

  @Nested
  class Bind {

    @Test
    public void happyPath() {
      DeleteDocumentQueryBuilder queryBuilder = new DeleteDocumentQueryBuilder(documentProperties);
      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName());

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);

      BatchQuery boundQuery = queryBuilder.bind(query, documentId, timestamp);

      assertThat(boundQuery.getValues().getValuesList())
          .containsExactly(Values.of(timestamp), Values.of(documentId));
    }
  }
}
