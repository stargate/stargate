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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.graphql.schema.graphqlfirst.admin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSource;
import io.stargate.sgv2.graphql.persistence.graphqlfirst.SchemaSourceDao;
import io.stargate.sgv2.graphql.schema.Uuids;
import io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin.AllSchemasFetcher;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AllSchemasFetcherTest {

  @Test
  public void getSchemasPerKeyspace() throws Exception {
    // given
    String keyspace = "ns_1";
    SchemaSource schemaSource1 = new SchemaSource(keyspace, Uuids.timeBased(), "content");
    SchemaSource schemaSource2 = new SchemaSource(keyspace, Uuids.timeBased(), "content-2");

    SchemaSourceDao schemaSourceDao = mock(SchemaSourceDao.class);
    when(schemaSourceDao.getAllVersions(keyspace))
        .thenReturn(Arrays.asList(schemaSource1, schemaSource2));
    AllSchemasFetcher allSchemasFetcher = new AllSchemasFetcher((ds) -> schemaSourceDao);

    DataFetchingEnvironment dataFetchingEnvironment = mockDataFetchingEnvironment(keyspace);

    StargateGraphqlContext context = mock(StargateGraphqlContext.class);
    StargateBridgeClient bridge = mockBridge(keyspace);
    when(context.getBridge()).thenReturn(bridge);

    // when
    List<SchemaSource> result = allSchemasFetcher.get(dataFetchingEnvironment, context);

    // then
    assertThat(result).containsExactly(schemaSource1, schemaSource2);
  }

  private DataFetchingEnvironment mockDataFetchingEnvironment(String keyspace) {
    DataFetchingEnvironment dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    when(dataFetchingEnvironment.getArgument("keyspace")).thenReturn(keyspace);
    return dataFetchingEnvironment;
  }

  private StargateBridgeClient mockBridge(String keyspace) {
    StargateBridgeClient bridge = mock(StargateBridgeClient.class);
    when(bridge.getKeyspace("ns_1", true))
        .thenReturn(Optional.of(CqlKeyspaceDescribe.newBuilder().build()));
    return bridge;
  }
}
