package io.stargate.sgv2.docsapi.service.schema.query;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.stargate.bridge.proto.QueryOuterClass;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class JsonSchemaQueryProviderTest {

  @Inject JsonSchemaQueryProvider queryProvider;

  @Nested
  class AttachSchemaQuery {
    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      String collection = RandomStringUtils.randomAlphanumeric(16);
      String schema = "{\"json\": 1}";

      QueryOuterClass.Query result = queryProvider.attachSchemaQuery(namespace, collection, schema);

      String cql =
          "ALTER TABLE \"%s\".\"%s\" WITH comment = '%s'".formatted(namespace, collection, schema);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
    }
  }
}
