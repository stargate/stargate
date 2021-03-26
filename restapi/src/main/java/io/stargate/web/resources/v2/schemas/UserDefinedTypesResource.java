package io.stargate.web.resources.v2.schemas;
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

import static io.stargate.web.docsapi.resources.RequestToHeadersMapper.getAllHeaders;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.Timed;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Table;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.models.ColumnDefinitionUserDefinedType;
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.models.UserDefinedTypeAdd;
import io.stargate.web.models.UserDefinedTypeResponse;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/**
 * Exposes REST Endpoint to work with Cassandra User Defined Types
 *
 * @see https://cassandra.apache.org/doc/latest/cql/types.html
 * @author Cedrick LUNVEN (@clunven)
 */
@Api(
    produces = APPLICATION_JSON,
    consumes = APPLICATION_JSON,
    tags = {"schemas"})
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Path("/v2/schemas/keyspaces/{keyspaceName}/types")
public class UserDefinedTypesResource {

  public static final String HEADER_TOKEN_AUTHENTICATION = "X-Cassandra-Token";
  public static final String RESOURCE_KEYSPACES = "/v2/schemas/keyspaces/";
  public static final String PATH_PARAM_KEYSPACE = "keyspaceName";

  /** Reference to the Data Store */
  @Inject private Db db;

  @Timed
  @GET
  @ApiOperation(
      value = "Get all user defined types (UDT). ",
      notes = "Retrieve all user defined types (UDT) in a specific keyspace.",
      response = ResponseWrapper.class,
      responseContainer = "List")
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = ResponseWrapper.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Keyspace has not been found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response findAll(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam(HEADER_TOKEN_AUTHENTICATION)
          String token,
      @ApiParam(value = "Keyspace to find all udts", required = true)
          @PathParam(PATH_PARAM_KEYSPACE)
          final String keyspaceName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          // Accessing datastore
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, getAllHeaders(request));
          // Load User Defined Type Definitions
          List<UserDefinedTypeResponse> udtResponses =
              authenticatedDB.getTypes(keyspaceName).stream()
                  .map(this::mapUdtAsReponse)
                  .collect(Collectors.toList());
          // Filter based on user credentials
          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  udtResponses.stream()
                      .map(UserDefinedTypeResponse::getName)
                      .collect(Collectors.toList()),
                  SourceAPI.REST,
                  ResourceKind.TYPE);
          // Wrap response if user does not asked for raw.
          Object response =
              raw ? udtResponses : new ResponseWrapper<List<UserDefinedTypeResponse>>(udtResponses);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get a user defined type (UDT) from its identifier",
      notes = "Retrieve data for a single table in a specific keyspace.",
      response = ResponseWrapper.class)
  @ApiResponses(
      value = {
        @ApiResponse(code = 200, message = "OK", response = Table.class),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 404, message = "Not Found", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{typeName}")
  public Response findById(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam(HEADER_TOKEN_AUTHENTICATION)
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(
              value = "Name of the user defined type (UDT) to use for the request.",
              required = true)
          @PathParam("typeName")
          final String typeName,
      @ApiParam(value = "Unwrap results", defaultValue = "false") @QueryParam("raw")
          final boolean raw,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          // Accessing datastore
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, getAllHeaders(request));
          // Permissions on a type are the same as keyspace (CreateTypeFetcher for graphQl API)
          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  null,
                  SourceAPI.REST,
                  ResourceKind.TYPE);
          // Mapping to Rest API expected output
          UserDefinedTypeResponse udtResponse =
              mapUdtAsReponse(authenticatedDB.getType(keyspaceName, typeName));
          return Response.ok(
                  Converters.writeResponse(
                      raw
                          ? udtResponse
                          : new ResponseWrapper<UserDefinedTypeResponse>(udtResponse)))
              .build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create a user defined type (UDT)",
      notes = "Add a user defined type (udt) in a specific keyspace.",
      response = Map.class,
      code = 201)
  @ApiResponses(
      value = {
        @ApiResponse(code = 201, message = "Created", response = Map.class),
        @ApiResponse(
            code = 400,
            message = "Bad Request, the input is not well formated",
            response = Error.class),
        @ApiResponse(
            code = 401,
            message = "Unauthorized, token is not valid or not enough permissions",
            response = Error.class),
        @ApiResponse(
            code = 409,
            message = "Conflict, the object may already exist",
            response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response createType(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam(HEADER_TOKEN_AUTHENTICATION)
          final String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "", required = true) @NotNull final UserDefinedTypeAdd udtAdd,
      @Context final HttpServletRequest request) {

    return RequestHandler.handle(
        () -> {
          // Accessing datastore
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, getAllHeaders(request));
          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);

          // Parameters validations, Keyspacename, type
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "keyspace does not exists", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }
          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  authenticatedDB.getAuthenticationSubject(),
                  keyspaceName,
                  null,
                  Scope.CREATE,
                  SourceAPI.REST,
                  ResourceKind.TYPE);
          String typeName = udtAdd.getName();
          if (typeName == null || typeName.equals("")) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "Type name must be provided", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }
          List<Column> columns = new ArrayList<>();
          for (ColumnDefinitionUserDefinedType colDef : udtAdd.getColumnDefinitions()) {
            String columnName = colDef.getName();
            if (columnName == null || columnName.equals("")) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(
                      new Error(
                          "column name must be provided",
                          Response.Status.BAD_REQUEST.getStatusCode()))
                  .build();
            }
            columns.add(
                Column.create(
                    columnName,
                    Kind.Regular,
                    Type.fromCqlDefinitionOf(keyspace, colDef.getTypeDefinition())));
          }
          // Build target object from definition
          UserDefinedType udt =
              ImmutableUserDefinedType.builder()
                  .keyspace(keyspaceName)
                  .name(typeName)
                  .addColumns((Column[]) columns.toArray(new Column[0]))
                  .build();
          // Execute creation
          authenticatedDB
              .getDataStore()
              .queryBuilder()
              .create()
              .type(keyspaceName, udt)
              .ifNotExists(udtAdd.isIfNotExists())
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();
          return Response.status(Response.Status.CREATED)
              .entity(Converters.writeResponse(Collections.singletonMap("name", typeName)))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(
      value = "Delete a User Defined type (UDT)",
      notes = "Delete a single user defined type (UDT) in the specified keyspace.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  @Path("/{typeName}")
  public Response delete(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam(HEADER_TOKEN_AUTHENTICATION)
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(
              value = "Name of the user defined type (UDT) to use for the request.",
              required = true)
          @PathParam("typeName")
          final String typeName,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          AuthenticatedDB authenticatedDB = db.getDataStoreForToken(token, getAllHeaders(request));

          // Permissions on a type are the same as keyspace
          // (CreateTypeFetcher for graphQl API)
          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  null,
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          authenticatedDB
              .getDataStore()
              .queryBuilder()
              .drop()
              .type(
                  keyspaceName,
                  ImmutableUserDefinedType.builder().keyspace(keyspaceName).name(typeName).build())
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();
          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  private UserDefinedTypeResponse mapUdtAsReponse(UserDefinedType udt) {
    return new UserDefinedTypeResponse(
        udt.name(),
        udt.keyspace(),
        udt.columns().stream().map(this::mapUdtColumnDefinition).collect(Collectors.toList()));
  }

  private ColumnDefinitionUserDefinedType mapUdtColumnDefinition(Column col) {
    return new ColumnDefinitionUserDefinedType(
        col.name(), null == col.type() ? null : col.type().cqlDefinition());
  }
}
