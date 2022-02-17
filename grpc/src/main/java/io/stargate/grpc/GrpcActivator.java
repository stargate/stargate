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
package io.stargate.grpc;

import io.stargate.auth.AuthenticationService;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.DbActivator;
import io.stargate.db.Persistence;
import io.stargate.grpc.impl.GrpcImpl;
import io.stargate.grpc.metrics.api.GrpcMetricsTagProvider;
import io.stargate.grpc.metrics.api.NoopGrpcMetricsTagProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class GrpcActivator extends BaseActivator {

  /**
   * Id if the {@link io.stargate.grpc.metrics.api.GrpcMetricsTagProvider}. If not set, this
   * activator will register a default impl.
   */
  private static final String GRPC_TAG_PROVIDER_ID =
      System.getProperty("stargate.metrics.grpc_tag_provider.id");

  private GrpcImpl grpc;
  private final ServicePointer<Metrics> metrics = ServicePointer.create(Metrics.class);
  private final ServicePointer<GrpcMetricsTagProvider> grpcTagProvider =
      ServicePointer.create(GrpcMetricsTagProvider.class);
  private final ServicePointer<AuthenticationService> authentication =
      ServicePointer.create(
          AuthenticationService.class,
          "AuthIdentifier",
          System.getProperty("stargate.auth_id", "AuthTableBasedService"));
  private final ServicePointer<Persistence> persistence =
      ServicePointer.create(Persistence.class, "Identifier", DbActivator.PERSISTENCE_IDENTIFIER);

  public GrpcActivator() {
    super("gRPC", true);
  }

  @Override
  protected List<ServiceAndProperties> createServices() {
    if (grpc != null) { // Already started
      return null;
    }

    GrpcMetricsTagProvider grpcMetricsTagProvider =
        Optional.ofNullable(grpcTagProvider.get()).orElseGet(NoopGrpcMetricsTagProvider::new);

    grpc =
        new GrpcImpl(
            persistence.get(), metrics.get(), authentication.get(), grpcMetricsTagProvider);
    grpc.start();

    return Collections.emptyList();
  }

  @Override
  protected void stopService() {
    if (grpc == null) { // Not started
      return;
    }
    grpc.stop();
    grpc = null;
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    ArrayList<ServicePointer<?>> dependencies = new ArrayList<>();
    dependencies.add(metrics);
    dependencies.add(persistence);
    dependencies.add(authentication);

    // depend on the gRPC tag provider only if it's set
    if (null != GRPC_TAG_PROVIDER_ID) {
      dependencies.add(grpcTagProvider);
    }

    return dependencies;
  }
}
