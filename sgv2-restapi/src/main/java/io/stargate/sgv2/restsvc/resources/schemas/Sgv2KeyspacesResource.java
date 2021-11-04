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
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.grpc.ExtProtoValueConverter;
import io.stargate.sgv2.restsvc.grpc.ExtProtoValueConverters;
import io.stargate.sgv2.restsvc.impl.GrpcClientFactory;
import io.stargate.sgv2.restsvc.models.RestServiceError;
import io.stargate.sgv2.restsvc.models.Sgv2Keyspace;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class Sgv2KeyspacesResource {
  // Singleton resource so no need to be static
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final ExtProtoValueConverters PROTOC_CONVERTERS = new ExtProtoValueConverters();

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
    /*
    return RequestHandler.handle(
        () -> {
          RestDB restDB = dbFactory.getRestDBForToken(token, getAllHeaders(request));

          List<Keyspace> keyspaces =
              restDB.getKeyspaces().stream()
                  .map(k -> new Keyspace(k.name(), buildDatacenters(k)))
                  .collect(Collectors.toList());

          restDB.authorizeSchemaRead(
              keyspaces.stream().map(Keyspace::getName).collect(Collectors.toList()),
              null,
              SourceAPI.REST,
              ResourceKind.KEYSPACE);

          Object response = raw ? keyspaces : new RESTResponseWrapper(keyspaces);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
       */
    return null;
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
    return null;
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
    Map<String, Object> replOptions = new HashMap<>();
    replOptions.put("replication_factor", 1);
    replOptions.put("class", "SimpleStrategy");

    String keyspaceName = payload.path("name").asText();
    if (keyspaceName == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(
              new RestServiceError(
                  "missing 'name' parameter in payload Object",
                  Response.Status.BAD_REQUEST.getStatusCode()))
          .build();
    }
    String cql =
        SchemaBuilder.createKeyspace(keyspaceName)
            .ifNotExists()
            .withReplicationOptions(replOptions)
            .asCql();

    logger.info("CREATE KEYSPACE with payload of: " + payload);
    logger.info("Trying to CREATE KEYSPACE with cql: [" + cql + "]");

    StargateGrpc.StargateBlockingStub blockingStub =
        grpcFactory.constructBlockingStub().withCallCredentials(new StargateBearerToken(token));
    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    // No real contents; can ignore ResultSet it seems and only worry about exceptions

    final Map<String, Object> responsePayload = Collections.singletonMap("name", keyspaceName);
    return javax.ws.rs.core.Response.status(Response.Status.CREATED)
        .entity(responsePayload)
        .build();
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
    return null;
  }

  private List<Map<String, Object>> convertRows(QueryOuterClass.ResultSet rs) {
    ExtProtoValueConverter converter = PROTOC_CONVERTERS.createConverter(rs.getColumnsList());
    List<Map<String, Object>> resultRows = new ArrayList<>();
    List<QueryOuterClass.Row> rows = rs.getRowsList();
    for (QueryOuterClass.Row row : rows) {
      resultRows.add(converter.fromProtoValues(row.getValuesList()));
    }
    return resultRows;
  }
}
