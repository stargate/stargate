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
package io.stargate.sgv2.restsvc.impl;

import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import io.stargate.sgv2.restsvc.resources.AuthenticationFilter;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import org.glassfish.hk2.api.Factory;

/**
 * Provides the stub that we created in {@link AuthenticationFilter} to the Jersey context, so that
 * it can be injected into resource methods with {@code @Context}.
 */
public class GrpcStubFactory implements Factory<StargateBlockingStub> {

  private final ContainerRequestContext context;

  @Inject
  public GrpcStubFactory(ContainerRequestContext context) {
    this.context = context;
  }

  @Override
  public StargateBlockingStub provide() {
    return (StargateBlockingStub) context.getProperty(AuthenticationFilter.GRPC_STUB_KEY);
  }

  @Override
  public void dispose(StargateBlockingStub instance) {
    // intentionally empty
  }
}
