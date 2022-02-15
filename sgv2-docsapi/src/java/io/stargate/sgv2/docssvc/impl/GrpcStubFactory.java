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
package java.io.stargate.sgv2.docssvc.impl;

import io.stargate.proto.StargateBridgeGrpc.StargateBridgeBlockingStub;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStubFilter;
import org.glassfish.hk2.api.Factory;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Provides the stub that we created in {@link CreateGrpcStubFilter} to the Jersey context, so that
 * it can be injected into resource methods with {@code @Context}.
 */
public class GrpcStubFactory implements Factory<StargateBridgeBlockingStub> {

  private final ContainerRequestContext context;

  @Inject
  public GrpcStubFactory(ContainerRequestContext context) {
    this.context = context;
  }

  @Override
  public StargateBridgeBlockingStub provide() {
    return (StargateBridgeBlockingStub) context.getProperty(CreateGrpcStubFilter.GRPC_STUB_KEY);
  }

  @Override
  public void dispose(StargateBridgeBlockingStub instance) {
    // intentionally empty
  }
}
