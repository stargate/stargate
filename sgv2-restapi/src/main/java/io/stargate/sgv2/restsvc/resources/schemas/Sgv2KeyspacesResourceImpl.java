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
package io.stargate.sgv2.restsvc.resources.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.Schema;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.restsvc.models.Sgv2Keyspace;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.resources.CreateStargateBridgeClient;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

@Path("/v2/schemas/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateStargateBridgeClient
public class Sgv2KeyspacesResourceImpl extends ResourceBase implements Sgv2KeyspacesResourceApi {
  private static final JsonMapper JSON_MAPPER = new JsonMapper();

  private static final SchemaBuilderHelper schemaBuilder = new SchemaBuilderHelper(JSON_MAPPER);

  @Override
  public Response getAllKeyspaces(
      final StargateBridgeClient bridge, final boolean raw, final HttpServletRequest request) {

    List<Sgv2Keyspace> keyspaces =
        bridge.getAllKeyspaces().stream()
            .map(Sgv2KeyspacesResourceImpl::keyspaceFrom)
            .collect(Collectors.toList());

    final Object payload = raw ? keyspaces : new Sgv2RESTResponse<>(keyspaces);
    return Response.status(Status.OK).entity(payload).build();
  }

  @Override
  public Response getOneKeyspace(
      final StargateBridgeClient bridge, final String keyspaceName, final boolean raw) {
    return bridge
        .getKeyspace(keyspaceName)
        .map(
            describe -> {
              Sgv2Keyspace keyspace = keyspaceFrom(describe);

              final Object payload = raw ? keyspace : new Sgv2RESTResponse<>(keyspace);
              return Response.status(Status.OK).entity(payload).build();
            })
        .orElseThrow(
            () -> new WebApplicationException("unable to describe keyspace", Status.NOT_FOUND));
  }

  @Override
  public Response createKeyspace(
      final StargateBridgeClient bridge, final JsonNode payload, final HttpServletRequest request) {
    SchemaBuilderHelper.KeyspaceCreateDefinition ksCreateDef;
    try {
      ksCreateDef = schemaBuilder.readKeyspaceCreateDefinition(payload);
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(e.getMessage(), Status.BAD_REQUEST);
    }
    final String keyspaceName = ksCreateDef.name;
    String cql;
    if (ksCreateDef.datacenters == null) {
      cql =
          new QueryBuilder()
              .create()
              .keyspace(keyspaceName)
              .ifNotExists()
              .withReplication(Replication.simpleStrategy(ksCreateDef.replicas))
              .build();
    } else {
      cql =
          new QueryBuilder()
              .create()
              .keyspace(keyspaceName)
              .ifNotExists()
              .withReplication(Replication.networkTopologyStrategy(ksCreateDef.datacenters))
              .build();
    }

    Query query = Query.newBuilder().setCql(cql).build();
    bridge.executeQuery(query);

    // No real contents; can ignore ResultSet it seems and only worry about exceptions

    final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspaceName);
    return Response.status(Status.CREATED).entity(responsePayload).build();
  }

  @Override
  public Response deleteKeyspace(
      final StargateBridgeClient bridge,
      final String keyspaceName,
      final HttpServletRequest request) {
    String cql = new QueryBuilder().drop().keyspace(keyspaceName).ifExists().build();
    Query query = Query.newBuilder().setCql(cql).build();
    /*QueryOuterClass.Response grpcResponse =*/ bridge.executeQuery(query);
    return Response.status(Status.NO_CONTENT).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for structural conversions
  /////////////////////////////////////////////////////////////////////////
   */

  private static Sgv2Keyspace keyspaceFrom(CqlKeyspaceDescribe describe) {
    Schema.CqlKeyspace keyspace = describe.getCqlKeyspace();
    Sgv2Keyspace ks = new Sgv2Keyspace(keyspace.getName());
    // TODO parse and convert "replication" option
    return ks;
  }
}
