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
package io.stargate.sgv2.docssvc.resources.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateBridgeGrpc;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.cql.builder.Replication;
import io.stargate.sgv2.restsvc.models.Sgv2Keyspace;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Path("/v2/schemas/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
@CreateGrpcStub
public class Sgv2KeyspacesResourceImpl extends ResourceBase implements Sgv2KeyspacesResourceApi {
  private static final JsonMapper JSON_MAPPER = new JsonMapper();

  private static final SchemaBuilderHelper schemaBuilder = new SchemaBuilderHelper(JSON_MAPPER);

  @Override
  public Response getAllKeyspaces(
      final StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      final boolean raw,
      final HttpServletRequest request) {

    String cql =
        new QueryBuilder()
            .select()
            .column("keyspace_name")
            .column("replication")
            .from("system_schema", "keyspaces")
            .build();

    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();

    // two-part conversion: first from proto to JsonNode for easier traversability,
    // then from that to actual response we need:
    ArrayNode ksRows = convertRowsToArrayNode(rs);
    List<Sgv2Keyspace> keyspaces = keyspacesFrom(ksRows);

    final Object payload = raw ? keyspaces : new Sgv2RESTResponse(keyspaces);
    return Response.status(Status.OK).entity(payload).build();
  }

  @Override
  public Response getOneKeyspace(
      final StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      final String keyspaceName,
      final boolean raw,
      final HttpServletRequest request) {
    String cql =
        new QueryBuilder()
            .select()
            .column("keyspace_name")
            .column("replication")
            .from("system_schema", "keyspaces")
            .where(BuiltCondition.of("keyspace_name", Predicate.EQ, keyspaceName))
            .build();

    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();

    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
    if (rs.getRowsCount() == 0) {
      throw new WebApplicationException("unable to describe keyspace", Status.NOT_FOUND);
    }
    // two-part conversion: first from proto to JsonNode for easier traversability,
    // then from that to actual response we need:
    ArrayNode ksRows = convertRowsToArrayNode(rs);
    Sgv2Keyspace keyspace = keyspaceFrom(ksRows.get(0));

    final Object payload = raw ? keyspace : new Sgv2RESTResponse(keyspace);
    return Response.status(Status.OK).entity(payload).build();
  }

  @Override
  public Response createKeyspace(
      final StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      final JsonNode payload,
      final HttpServletRequest request) {
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

    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    // No real contents; can ignore ResultSet it seems and only worry about exceptions

    final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspaceName);
    return Response.status(Status.CREATED).entity(responsePayload).build();
  }

  @Override
  public Response deleteKeyspace(
      final StargateBridgeGrpc.StargateBridgeBlockingStub blockingStub,
      final String keyspaceName,
      final HttpServletRequest request) {
    String cql = new QueryBuilder().drop().keyspace(keyspaceName).ifExists().build();
    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    return Response.status(Status.NO_CONTENT).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods for structural conversions
  /////////////////////////////////////////////////////////////////////////
   */

  private static List<Sgv2Keyspace> keyspacesFrom(JsonNode ksRootNode) {
    List<Sgv2Keyspace> result =
        StreamSupport.stream(ksRootNode.spliterator(), false)
            .map(x -> keyspaceFrom(x))
            .collect(Collectors.toList());
    return result;
  }

  private static Sgv2Keyspace keyspaceFrom(JsonNode ksNode) {
    final String ksName = ksNode.path("keyspace_name").asText();
    final Sgv2Keyspace ks = new Sgv2Keyspace(ksName);

    // 09-Nov-2021, tatu: Below is what should work correctly, as per documentation,
    //   but that does NOT indeed work, nor produce output as documented by Swagger.
    //   Stargate V1 has same issues (see https://github.com/stargate/stargate/issues/1396)
    //   so for now will simply be compatible with V1, but not correct.

    /*
    Iterator<Map.Entry<String, JsonNode>> it = ksNode.path("replication").fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      final String dcName = entry.getKey();
      // Datacenters are exposed as Map/Object entries from key to replica count,
      // plus at least one meta-entry ("class") for replication strategy
      if ("class".equals(dcName)) {
        continue;
      }
      ks.addDatacenter(dcName, entry.getValue().asInt(0));
    }
     */
    return ks;
  }
}
