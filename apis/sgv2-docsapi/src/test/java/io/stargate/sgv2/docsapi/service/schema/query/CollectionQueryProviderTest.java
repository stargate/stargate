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
package io.stargate.sgv2.docsapi.service.schema.query;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.docsapi.testprofiles.SaiEnabledTestProfile;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(CollectionQueryProviderTest.Profile.class)
class CollectionQueryProviderTest {

  public static class Profile implements NoGlobalResourcesTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      // adapt consistency, depth and column name(s)
      return ImmutableMap.<String, String>builder()
          .put("stargate.queries.consistency.schema-changes", "ONE")
          .put("stargate.document.max-depth", "4")
          .put("stargate.document.table.key-column-name", "k")
          .put("stargate.document.table.leaf-column-name", "l")
          .build();
    }
  }

  @Inject CollectionQueryProvider queryProvider;

  @Nested
  class CreateCollectionQuery {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      QueryOuterClass.Query result = queryProvider.createCollectionQuery(namespace, collection);

      String cql =
          "CREATE TABLE \"%s\".\"%s\" (k text, p0 text, p1 text, p2 text, p3 text, l text, text_value text, dbl_value double, bool_value boolean, PRIMARY KEY ((k), p0, p1, p2, p3))"
              .formatted(namespace, collection);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.ONE);
    }
  }

  @Nested
  @TestProfile(CreateCollectionQueryWithNumericBooleans.Profile.class)
  class CreateCollectionQueryWithNumericBooleans {

    public static class Profile extends SaiEnabledTestProfile {

      @Override
      public Map<String, String> getConfigOverrides() {
        // adapt consistency, depth and column name(s)
        return ImmutableMap.<String, String>builder()
            .putAll(super.getConfigOverrides())
            .put("stargate.document.max-depth", "4")
            .build();
      }
    }

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      QueryOuterClass.Query result = queryProvider.createCollectionQuery(namespace, collection);

      String cql =
          "CREATE TABLE \"%s\".\"%s\" (key text, p0 text, p1 text, p2 text, p3 text, leaf text, text_value text, dbl_value double, bool_value tinyint, PRIMARY KEY ((key), p0, p1, p2, p3))"
              .formatted(namespace, collection);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
    }
  }

  @Nested
  class DeleteCollectionQuery {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      QueryOuterClass.Query result = queryProvider.deleteCollectionQuery(namespace, collection);

      String cql = "DROP TABLE \"%s\".\"%s\"".formatted(namespace, collection);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.ONE);
    }
  }

  @Nested
  class CreateCollectionIndexQueries {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      List<QueryOuterClass.Query> result =
          queryProvider.createCollectionIndexQueries(namespace, collection);

      assertThat(result)
          .hasSize(4)
          .allSatisfy(
              q -> {
                assertThat(q.getValues().getValuesCount()).isZero();
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.ONE);
              })
          .flatMap(QueryOuterClass.Query::getCql)
          .contains(
              "CREATE INDEX IF NOT EXISTS ON \"%s\".\"%s\" (l)".formatted(namespace, collection))
          .contains(
              "CREATE INDEX IF NOT EXISTS ON \"%s\".\"%s\" (text_value)"
                  .formatted(namespace, collection))
          .contains(
              "CREATE INDEX IF NOT EXISTS ON \"%s\".\"%s\" (dbl_value)"
                  .formatted(namespace, collection))
          .contains(
              "CREATE INDEX IF NOT EXISTS ON \"%s\".\"%s\" (bool_value)"
                  .formatted(namespace, collection));
    }
  }

  @Nested
  @TestProfile(SaiEnabledTestProfile.class)
  class CreateCollectionIndexQueriesWithSaiEnabled {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);

      List<QueryOuterClass.Query> result =
          queryProvider.createCollectionIndexQueries(namespace, collection);

      assertThat(result)
          .hasSize(4)
          .allSatisfy(
              q -> {
                assertThat(q.getValues().getValuesCount()).isZero();
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
              })
          .flatMap(QueryOuterClass.Query::getCql)
          .contains(
              "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%s\".\"%s\" (leaf) USING 'StorageAttachedIndex'"
                  .formatted(namespace, collection))
          .contains(
              "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%s\".\"%s\" (text_value) USING 'StorageAttachedIndex'"
                  .formatted(namespace, collection))
          .contains(
              "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%s\".\"%s\" (dbl_value) USING 'StorageAttachedIndex'"
                  .formatted(namespace, collection))
          .contains(
              "CREATE CUSTOM INDEX IF NOT EXISTS ON \"%s\".\"%s\" (bool_value) USING 'StorageAttachedIndex'"
                  .formatted(namespace, collection));
    }
  }
}
