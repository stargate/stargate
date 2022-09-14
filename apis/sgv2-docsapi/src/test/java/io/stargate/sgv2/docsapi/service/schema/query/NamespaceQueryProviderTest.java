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
import io.stargate.sgv2.api.common.cql.builder.Replication;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class NamespaceQueryProviderTest {

  @Inject NamespaceQueryProvider queryProvider;

  @Nested
  class CreateNamespaceQuery {

    @Test
    public void simpleStrategy() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      Replication replication = Replication.simpleStrategy(2);

      QueryOuterClass.Query result = queryProvider.createNamespaceQuery(namespace, replication);

      String cql =
          "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2 }"
              .formatted(namespace);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
    }

    @Test
    public void networkTopologyStrategy() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);
      Replication replication =
          Replication.networkTopologyStrategy(ImmutableMap.of("dc1", 2, "dc2", 3));

      QueryOuterClass.Query result = queryProvider.createNamespaceQuery(namespace, replication);

      String cql =
          "CREATE KEYSPACE IF NOT EXISTS \"%s\" WITH replication = { 'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 3 }"
              .formatted(namespace);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
    }
  }

  @Nested
  class DeleteNamespaceQuery {

    @Test
    public void happyPath() {
      String namespace = RandomStringUtils.randomAlphanumeric(16);

      QueryOuterClass.Query result = queryProvider.deleteNamespaceQuery(namespace);

      String cql = "DROP KEYSPACE \"%s\"".formatted(namespace);
      assertThat(result.getCql()).isEqualTo(cql);
      assertThat(result.getValues().getValuesCount()).isZero();
      assertThat(result.getParameters().getConsistency().getValue())
          .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
    }
  }
}
