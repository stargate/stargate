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

package io.stargate.sgv2.api.common;

import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.grpc.RetriableStargateBridge;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.stargate.sgv2.api.common.token.CassandraTokenResolver;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;

/**
 * The request information containing the tenant ID and the Cassandra key, and the eagerly created
 * {@link StargateBridge} to be used in this request. This bean is @{@link RequestScoped}.
 *
 * <p>Uses the registered {@link TenantResolver} and {@link CassandraTokenResolver} to optionally
 * resolve the tenant ID and the Cassandra token.
 */
@RequestScoped
public class StargateRequestInfo {

  private final Optional<String> tenantId;

  private final Optional<String> cassandraToken;

  private final StargateBridge stargateBridge;

  @Inject
  public StargateRequestInfo(
      RoutingContext routingContext,
      SecurityContext securityContext,
      @Retriable RetriableStargateBridge bridge,
      Instance<TenantResolver> tenantResolver,
      Instance<CassandraTokenResolver> tokenResolver) {
    this.tenantId = tenantResolver.get().resolve(routingContext, securityContext);
    this.cassandraToken = tokenResolver.get().resolve(routingContext, securityContext);
    this.stargateBridge = bridge;
  }

  public Optional<String> getTenantId() {
    return tenantId;
  }

  public Optional<String> getCassandraToken() {
    return cassandraToken;
  }

  public StargateBridge getStargateBridge() {
    return stargateBridge;
  }
}
