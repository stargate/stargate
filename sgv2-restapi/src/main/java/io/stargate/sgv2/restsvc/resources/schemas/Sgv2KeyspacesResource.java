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

import com.codahale.metrics.annotation.Timed;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.ExtProtoValueConverters;
import io.stargate.sgv2.restsvc.grpc.FromProtoConverter;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2Keyspace;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import io.stargate.sgv2.restsvc.resources.Sgv2RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/schemas/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Sgv2KeyspacesResource extends ResourceBase {
  private static final String DC_META_ENTRY_CLASS = "class";

  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final JsonMapper JSON_MAPPER = new JsonMapper();

  private static final SchemaBuilderHelper schemaBuilder = new SchemaBuilderHelper(JSON_MAPPER);

  /** Entity used to connect to backend gRPC service. */
  @Inject private GrpcClientFactory grpcFactory;

  @Timed
  @GET
  @ApiOperation(
      value = "Get all keyspaces",
      notes = "Retrieve all available keyspaces.",
      response = Sgv2Keyspace.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Keyspace.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  public Response getAllKeyspaces(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }

    StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    QueryOuterClass.QueryParameters.Builder paramsB = QueryOuterClass.QueryParameters.newBuilder();

    String cql =
        QueryBuilder.selectFrom("system_schema", "keyspaces")
            .column("keyspace_name")
            .column("replication")
            .asCql();

    logger.info("getAllKeyspaces, cql = " + cql);

    return Sgv2RequestHandler.handle(
        () -> {
          QueryOuterClass.Query query =
              QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql).build();
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

          final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();

          // two-part conversion: first from proto to JsonNode for easier traversability,
          // then from that to actual response we need:
          ArrayNode ksRows = convertRowsToJsonNode(rs);
          List<Sgv2Keyspace> keyspaces = keyspacesFrom(ksRows);

          final Object payload = raw ? keyspaces : new Sgv2RESTResponse(keyspaces);
          return jaxrsResponse(Response.Status.OK).entity(payload).build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get a keyspace",
      notes = "Return a single keyspace specification.",
      response = Sgv2Keyspace.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Sgv2Keyspace.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 404, message = "Not Found", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{keyspaceName}")
  public Response getOneKeyspace(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }

    StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    QueryOuterClass.QueryParameters.Builder paramsB = QueryOuterClass.QueryParameters.newBuilder();

    String cql =
        QueryBuilder.selectFrom("system_schema", "keyspaces")
            .column("keyspace_name")
            .column("replication")
            .whereColumn("keyspace_name")
            .isEqualTo(QueryBuilder.literal(keyspaceName))
            .asCql();

    logger.info("getOneKeyspace, cql = " + cql);

    QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder().setParameters(paramsB.build()).setCql(cql).build();

    return Sgv2RequestHandler.handle(
        () -> {
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

          final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();
          if (rs.getRowsCount() == 0) {
            return jaxrsResponse(Response.Status.NOT_FOUND)
                .entity(
                    new RestServiceError(
                        "unable to describe keyspace", Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }
          // two-part conversion: first from proto to JsonNode for easier traversability,
          // then from that to actual response we need:
          ArrayNode ksRows = convertRowsToJsonNode(rs);
          Sgv2Keyspace keyspace = keyspaceFrom(ksRows.get(0));

          final Object payload = raw ? keyspace : new Sgv2RESTResponse(keyspace);
          return jaxrsResponse(Response.Status.OK).entity(payload).build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create a keyspace",
      notes = "Create a new keyspace.",
      response = Map.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(code = 400, message = "Bad Request", response = RestServiceError.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(code = 409, message = "Conflict", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  public Response createKeyspace(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(
              value =
                  "A map representing a keyspace with SimpleStrategy or NetworkTopologyStrategy with default replicas of 1 and 3 respectively \n"
                      + "Simple:\n"
                      + "```json\n"
                      + "{ \"name\": \"killrvideo\", \"replicas\": 1}\n"
                      + "````\n"
                      + "Network Topology:\n"
                      + "```json\n"
                      + "{\n"
                      + "  \"name\": \"killrvideo\",\n"
                      + "   \"datacenters\":\n"
                      + "      [\n"
                      + "         { \"name\": \"dc1\", \"replicas\": 3 },\n"
                      + "         { \"name\": \"dc2\", \"replicas\": 3 },\n"
                      + "      ],\n"
                      + "}\n"
                      + "```")
          JsonNode payload,
      @Context HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }
    return Sgv2RequestHandler.handle(
        () -> {
          SchemaBuilderHelper.KeyspaceCreateDefinition ksCreateDef;
          try {
            ksCreateDef = schemaBuilder.readKeyspaceCreateDefinition(payload);
          } catch (IllegalArgumentException e) {
            return jaxrsResponse(Response.Status.BAD_REQUEST)
                .entity(
                    new RestServiceError(
                        e.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }
          final String keyspaceName = ksCreateDef.name;
          CreateKeyspaceStart start = SchemaBuilder.createKeyspace(keyspaceName).ifNotExists();
          String cql;
          if (ksCreateDef.datacenters == null) {
            cql = start.withSimpleStrategy(ksCreateDef.replicas).asCql();
          } else {
            cql = start.withNetworkTopologyStrategy(ksCreateDef.datacenters).asCql();
          }

          logger.info("Sending CREATE KEYSPACE with cql: [" + cql + "]");

          StargateGrpc.StargateBlockingStub blockingStub =
              grpcFactory
                  .constructBlockingStub()
                  .withCallCredentials(new StargateBearerToken(token));
          QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
          QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

          // No real contents; can ignore ResultSet it seems and only worry about exceptions

          final Map<String, Object> responsePayload =
              Collections.singletonMap("name", keyspaceName);
          return jaxrsResponse(Response.Status.CREATED).entity(responsePayload).build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a keyspace", notes = "Delete a single keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = RestServiceError.class),
        @ApiResponse(
            code = 500,
            message = "Internal server error",
            response = RestServiceError.class)
      })
  @Path("/{keyspaceName}")
  public Response deleteKeyspace(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @Context HttpServletRequest request) {
    if (isAuthTokenInvalid(token)) {
      return invalidTokenFailure();
    }

    // !!! TO IMPLEMENT
    return jaxrsResponse(Response.Status.NOT_IMPLEMENTED).entity(Collections.emptyMap()).build();
  }

  private ArrayNode convertRowsToJsonNode(QueryOuterClass.ResultSet rs) {
    FromProtoConverter converter =
        ExtProtoValueConverters.instance().createConverter(rs.getColumnsList());
    ArrayNode resultRows = JSON_MAPPER.createArrayNode();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.objectNodeFromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }

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
      if (DC_META_ENTRY_CLASS.equals(dcName)) {
        continue;
      }
      ks.addDatacenter(dcName, entry.getValue().asInt(0));
    }
     */
    return ks;
  }
}
