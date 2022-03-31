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
package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.it.http.ApiServiceConnectionInfo;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.graphql.BaseGraphqlV2ApiTest;
import io.stargate.it.storage.ClusterSpec;
import io.stargate.it.storage.StargateConnectionInfo;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

// Redeclare ClusterSpec to get shared=false; this test must be isolated because it will fail if
// other tests have created their own user keyspaces before it.
@ClusterSpec
public class DefaultKeyspaceTest extends BaseGraphqlV2ApiTest {

  @Test
  @DisplayName("Should fail to query default keyspace when there is none")
  public void queryDefaultKeyspaceWhenMissing(
      StargateConnectionInfo stargateBackend, ApiServiceConnectionInfo stargateGraphqlApi) {
    // Given
    CqlFirstClient client =
        new CqlFirstClient(
            stargateGraphqlApi.host(),
            stargateGraphqlApi.port(),
            RestUtils.getAuthToken(stargateBackend.seedAddress()));

    // When
    String error = client.getDmlQueryError(null, "{}", HttpStatus.SC_NOT_FOUND);

    // Then
    assertThat(error).isEqualTo("No default keyspace defined");
  }
}
