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

package io.stargate.sgv2.docsapi.api.v2.example;

import io.smallrye.mutiny.Uni;
import io.stargate.proto.MutinyStargateBridgeGrpc;
import io.stargate.proto.Schema;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.v2.example.model.dto.KeyspaceExistsDto;
import io.stargate.sgv2.docsapi.config.StargateConfig;
import io.stargate.sgv2.docsapi.grpc.GrpcClients;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Optional;

@Path("/api/v2/example")
@Produces(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = StargateConfig.Constants.OPEN_API_DEFAULT_SECURITY_SCHEME)
@Slf4j
public class ExampleResource {

    @Inject
    GrpcClients grpcClients;

    @Inject
    StargateRequestInfo requestInfo;

    @GET
    @Path("keyspace-exists/{name}")
    public Uni<KeyspaceExistsDto> keyspaceExists(@PathParam("name") String name) {
        Schema.DescribeKeyspaceQuery describeKeyspaceQuery = Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(name).build();

        return grpcClients.bridgeClient(requestInfo)
                .describeKeyspace(describeKeyspaceQuery)
                .map(r -> new KeyspaceExistsDto("name", true));
    }
}