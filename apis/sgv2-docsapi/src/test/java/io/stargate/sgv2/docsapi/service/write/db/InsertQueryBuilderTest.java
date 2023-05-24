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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import io.stargate.sgv2.docsapi.testprofiles.MaxDepth8TestProfile;
import jakarta.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(MaxDepth8TestProfile.class)
class InsertQueryBuilderTest {

  @Inject DocumentProperties documentProperties;
  @Inject DocsApiTestSchemaProvider schemaProvider;

  @Nested
  class BuildQuery {

    @Test
    public void happyPath() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);

      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName(), null);

      String expected =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName());
      assertThat(query.getCql()).isEqualTo(expected);
    }

    @Test
    public void happyPathWithTtl() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);

      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName(), 1);

      String expected =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?",
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName());
      assertThat(query.getCql()).isEqualTo(expected);
    }
  }

  @Nested
  class Bind {

    @Test
    public void stringValue() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);
      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName(), null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      String value = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("first")
              .addPath("second")
              .stringValue(value)
              .build();

      query = queryBuilder.bind(query, documentId, row, null, timestamp, false);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of(documentId),
              Values.of("first"),
              Values.of("second"),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of("second"),
              Values.of(value),
              Values.NULL,
              Values.NULL,
              Values.of(timestamp));
    }

    @Test
    public void doubleValue() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);
      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName(), null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      double value = RandomUtils.nextDouble();
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("first")
              .addPath("second")
              .doubleValue(value)
              .build();

      query = queryBuilder.bind(query, documentId, row, null, timestamp, false);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of(documentId),
              Values.of("first"),
              Values.of("second"),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of("second"),
              Values.NULL,
              Values.of(value),
              Values.NULL,
              Values.of(timestamp));
    }

    @Test
    public void booleanValue() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);
      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName(), null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      boolean value = RandomUtils.nextBoolean();
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(documentProperties.maxDepth())
              .addPath("first")
              .addPath("second")
              .booleanValue(value)
              .build();

      query = queryBuilder.bind(query, documentId, row, null, timestamp, false);
      assertThat(query.getValues().getValuesList())
          .containsExactly(
              Values.of(documentId),
              Values.of("first"),
              Values.of("second"),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of(""),
              Values.of("second"),
              Values.NULL,
              Values.NULL,
              Values.of(value),
              Values.of(timestamp));
    }

    @Test
    public void maxDepthDifferent() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);
      BatchQuery query =
          queryBuilder.buildQuery(
              schemaProvider.getKeyspace().getName(), schemaProvider.getTable().getName(), null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(4)
              .addPath("first")
              .booleanValue(true)
              .build();

      assertThatThrownBy(() -> queryBuilder.bind(query, documentId, row, null, timestamp, true))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
