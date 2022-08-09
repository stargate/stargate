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
package io.stargate.sgv2.graphql.integration.util;

import com.google.protobuf.StringValue;
import io.quarkus.test.common.http.TestHTTPResource;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import java.time.Duration;
import java.util.Arrays;
import javax.inject.Inject;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class GraphqlIntegrationTest {

  protected final String keyspaceName = "ks" + RandomStringUtils.randomNumeric(16);
  @Inject protected StargateRequestInfo requestInfo;
  protected StargateBridge bridge;

  @TestHTTPResource protected String baseUrl;

  @BeforeAll
  public void createBridge() {
    bridge = requestInfo.getStargateBridge();
  }

  @BeforeAll
  public void createKeyspace() {
    executeCql(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            .formatted(keyspaceName));
  }

  @AfterAll
  public void dropKeyspace() {
    executeCql("DROP KEYSPACE %s".formatted(keyspaceName));
  }

  protected QueryOuterClass.Response executeCql(String cql, QueryOuterClass.Value... values) {
    QueryParameters parameters =
        cql.startsWith("CREATE KEYSPACE")
            ? QueryParameters.getDefaultInstance()
            : QueryParameters.newBuilder().setKeyspace(StringValue.of(keyspaceName)).build();
    Query.Builder query = Query.newBuilder().setCql(cql).setParameters(parameters);
    if (values.length > 0) {
      query.setValues(QueryOuterClass.Values.newBuilder().addAllValues(Arrays.asList(values)));
    }
    return bridge.executeQuery(query.build()).await().atMost(Duration.ofSeconds(10));
  }
}
