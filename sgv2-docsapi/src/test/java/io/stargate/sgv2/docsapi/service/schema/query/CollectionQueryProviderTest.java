package io.stargate.sgv2.docsapi.service.schema.query;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.testprofiles.SaiEnabledTestProfile;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(CollectionQueryProviderTest.Profile.class)
class CollectionQueryProviderTest {

  public static class Profile implements QuarkusTestProfile {
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
              "CREATE INDEX IF NOT EXISTS ON \"%s\".\"%s\" (bool_value)"
                  .formatted(namespace, collection));
    }
  }
}
