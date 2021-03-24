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
package io.stargate.web.resources.v2.schemas;

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.query.builder.Replication;
import io.stargate.web.models.Datacenter;
import io.stargate.web.models.Error;
import io.stargate.web.models.Keyspace;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
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
import java.util.stream.Collectors;
import javax.inject.Inject;
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
import org.apache.cassandra.stargate.db.ConsistencyLevel;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/schemas/keyspaces")
@Produces(MediaType.APPLICATION_JSON)
public class KeyspacesResource {
  @Inject private Db db;
  private static final ObjectMapper mapper = new ObjectMapper();

  @Timed
  @GET
  @ApiOperation(
      value = "Get all keyspaces",
      notes = "Retrieve all available keyspaces.",
      response = ResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          List<Keyspace> keyspaces =
              authenticatedDB.getKeyspaces().stream()
                  .map(k -> new Keyspace(k.name(), buildDatacenters(k)))
                  .collect(Collectors.toList());

          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  keyspaces.stream().map(Keyspace::getName).collect(Collectors.toList()),
                  null,
                  SourceAPI.REST,
                  ResourceKind.KEYSPACE);

          Object response = raw ? keyspaces : new ResponseWrapper(keyspaces);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get a keyspace",
      notes = "Return a single keyspace specification.",
      response = Keyspace.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Keyspace.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);
          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  null,
                  SourceAPI.REST,
                  ResourceKind.KEYSPACE);

          io.stargate.db.schema.Keyspace keyspace = authenticatedDB.getKeyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        "unable to describe keyspace", Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          Keyspace keyspaceResponse = new Keyspace(keyspace.name(), buildDatacenters(keyspace));

          Object response = raw ? keyspaceResponse : new ResponseWrapper(keyspaceResponse);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
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
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
          String payload,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          Map<String, Object> requestBody = mapper.readValue(payload, Map.class);

          String keyspaceName = (String) requestBody.get("name");
          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  authenticatedDB.getAuthenticationSubject(),
                  keyspaceName,
                  null,
                  Scope.CREATE,
                  SourceAPI.REST,
                  ResourceKind.KEYSPACE);

          Replication replication;
          if (requestBody.containsKey("datacenters")) {
            ArrayList<?> datacenters = (ArrayList<?>) requestBody.get("datacenters");
            Map<String, Integer> dcReplications = new HashMap<>();
            for (Object dc : datacenters) {
              String dcName = (String) ((Map<?, ?>) dc).get("name");
              Integer replicas =
                  ((Map<?, ?>) dc).containsKey("replicas")
                      ? (Integer) ((Map<?, ?>) dc).get("replicas")
                      : 3;
              dcReplications.put(dcName, replicas);
            }
            replication = Replication.networkTopologyStrategy(dcReplications);
          } else {
            replication = Replication.simpleStrategy((int) requestBody.getOrDefault("replicas", 1));
          }

          authenticatedDB
              .getDataStore()
              .queryBuilder()
              .create()
              .keyspace(keyspaceName)
              .ifNotExists()
              .withReplication(replication)
              .build()
              .execute()
              .get();

          return Response.status(Response.Status.CREATED)
              .entity(Converters.writeResponse(Collections.singletonMap("name", keyspaceName)))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a keyspace", notes = "Delete a single keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
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
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  authenticatedDB.getAuthenticationSubject(),
                  keyspaceName,
                  null,
                  Scope.DROP,
                  SourceAPI.REST,
                  ResourceKind.KEYSPACE);

          authenticatedDB
              .getDataStore()
              .queryBuilder()
              .drop()
              .keyspace(keyspaceName)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  private List<Datacenter> buildDatacenters(io.stargate.db.schema.Keyspace keyspace) {
    List<Datacenter> dcs = new ArrayList<>();
    for (Map.Entry<String, String> entries : keyspace.replication().entrySet()) {
      if (entries.getKey().equals("class") || entries.getKey().equals("replication_factor")) {
        continue;
      }

      dcs.add(new Datacenter(entries.getKey(), Integer.parseInt(entries.getValue())));
    }

    return dcs.isEmpty() ? null : dcs;
  }
}
