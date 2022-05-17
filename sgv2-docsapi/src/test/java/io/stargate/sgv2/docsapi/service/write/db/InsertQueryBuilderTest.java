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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.sgv2.docsapi.DocsApiTestSchemaProvider;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.sgv2.docsapi.service.JsonShreddedRow;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(InsertQueryBuilderTest.Config.class)
class InsertQueryBuilderTest {

  private static final int MAX_DEPTH = 8;
  private static final DocsApiTestSchemaProvider SCHEMA_PROVIDER =
      new DocsApiTestSchemaProvider(MAX_DEPTH);
  private static final String KEYSPACE_NAME = SCHEMA_PROVIDER.getKeyspace().getName();
  private static final String COLLECTION_NAME = SCHEMA_PROVIDER.getTable().getName();

  public static class Config implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("stargate.document.max-depth", Integer.toString(MAX_DEPTH));
    }
  }

  @Inject DocumentProperties documentProperties;

  @Nested
  class BuildQuery {

    @Test
    public void happyPath() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);

      Query query = queryBuilder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, null);

      String expected =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
    }

    @Test
    public void happyPathWithTtl() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);

      Query query = queryBuilder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, 1);

      String expected =
          String.format(
              "INSERT INTO %s.%s (key, p0, p1, p2, p3, p4, p5, p6, p7, leaf, text_value, dbl_value, bool_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL 1 AND TIMESTAMP ?",
              KEYSPACE_NAME, COLLECTION_NAME);
      assertThat(query.getCql()).isEqualTo(expected);
    }
  }

  @Nested
  class Bind {

    @Test
    public void stringValue() {
      InsertQueryBuilder queryBuilder = new InsertQueryBuilder(documentProperties);
      Query query = queryBuilder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, null);

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

      query = queryBuilder.bind(query, documentId, row, timestamp, false);
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
      Query query = queryBuilder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, null);

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

      query = queryBuilder.bind(query, documentId, row, timestamp, false);
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
      Query query = queryBuilder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, null);

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

      query = queryBuilder.bind(query, documentId, row, timestamp, false);
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
      Query query = queryBuilder.buildQuery(KEYSPACE_NAME, COLLECTION_NAME, null);

      long timestamp = RandomUtils.nextLong();
      String documentId = RandomStringUtils.randomAlphanumeric(16);
      JsonShreddedRow row =
          ImmutableJsonShreddedRow.builder()
              .maxDepth(4)
              .addPath("first")
              .booleanValue(true)
              .build();

      assertThatThrownBy(() -> queryBuilder.bind(query, documentId, row, timestamp, true))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
