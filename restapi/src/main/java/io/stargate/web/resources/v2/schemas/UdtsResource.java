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
import com.google.common.collect.Maps;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.models.Error;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.UdtAdd;
import io.stargate.web.resources.AuthenticatedDB;
import io.stargate.web.resources.Converters;
import io.stargate.web.resources.Db;
import io.stargate.web.resources.RequestHandler;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.NotFoundException;
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
@Path("/v2/schemas/keyspaces/{keyspaceName}/types")
@Produces(MediaType.APPLICATION_JSON)
public class UdtsResource {

  @Inject private Db db;

  @Timed
  @GET
  @ApiOperation(
      value = "Get all Types",
      notes = "Get all Types from a given keyspace",
      response = GetResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = GetResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response listAllUdts(
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
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  Collections.singletonList(null),
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          Map<String, Map<String, String>> response =
              authenticatedDB.getDataStore().schema().keyspace(keyspaceName).userDefinedTypes()
                  .stream()
                  .collect(Collectors.toMap(UserDefinedType::name, Converters::convertColumnMap));

          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get an UDT by name",
      notes = "Get an UDT by name from a given keyspace",
      response = GetResponseWrapper.class,
      responseContainer = "Map")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = GetResponseWrapper.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{typeName}")
  public Response getUdt(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the type.", required = true) @PathParam("typeName")
          final String typeName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          Map<String, String> allHeaders = getAllHeaders(request);
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, allHeaders);

          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  Collections.singletonList(null),
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          UserDefinedType udt =
              authenticatedDB
                  .getDataStore()
                  .schema()
                  .keyspace(keyspaceName)
                  .userDefinedType(typeName);

          if (udt == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("The type %s.%s was not found.", keyspaceName, typeName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          Map<String, Map<String, String>> response = Maps.newHashMap();
          response.put(udt.name(), Converters.convertColumnMap(udt));

          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete an UDT", notes = "Delete a udt in the specified keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{typeName}")
  public Response deleteUdt(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Name of the udt.", required = true) @PathParam("typeName")
          final String typeName,
      @ApiParam(
              defaultValue = "false",
              value =
                  "Attempting to drop a non existing udt returns an error unless this option is true.")
          @QueryParam("ifExists")
          final boolean ifExists,
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
                  ResourceKind.TYPE);

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          try {
            authenticatedDB
                .getDataStore()
                .queryBuilder()
                .drop()
                .type(keyspaceName, UserDefinedType.reference(keyspaceName, typeName))
                .ifExists(ifExists)
                .build()
                .execute(ConsistencyLevel.LOCAL_QUORUM)
                .get();
          } catch (IllegalArgumentException | NotFoundException iae) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("%s", iae.getMessage()),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create an UDT",
      notes = "Create an UDT in the given keyspace",
      response = String.class,
      responseContainer = "Map",
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(
            code = 201,
            message = "resource created",
            response = Map.class,
            responseContainer = "Map"),
        @ApiResponse(code = 400, message = "Bad Request", response = Error.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 409, message = "Conflict", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{typeName}")
  public Response createUdt(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam("X-Cassandra-Token")
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "Type name", required = true) @PathParam("typeName") final String typeName,
      @ApiParam(required = true) @NotNull final UdtAdd udtAdd,
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
                  Scope.CREATE,
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                .entity(
                    new Error(
                        String.format("Keyspace '%s' not found.", keyspaceName),
                        Response.Status.NOT_FOUND.getStatusCode()))
                .build();
          }

          if (udtAdd.getFields() == null || udtAdd.getFields().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format("A UDT definition should incluse the fields definition."),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          try {
            List<Column> fields = Converters.fromUdtAdd(udtAdd);

            UserDefinedType udt =
                ImmutableUserDefinedType.builder()
                    .keyspace(keyspaceName)
                    .name(typeName)
                    .addAllColumns(fields)
                    .build();

            authenticatedDB
                .getDataStore()
                .queryBuilder()
                .create()
                .type(keyspaceName, udt)
                .ifNotExists(udtAdd.getIfNotExists())
                .build()
                .execute(ConsistencyLevel.LOCAL_QUORUM)
                .get();
          } catch (IllegalArgumentException iae) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        String.format(iae.getMessage()),
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          return Response.status(Response.Status.CREATED).build();
        });
  }
}
