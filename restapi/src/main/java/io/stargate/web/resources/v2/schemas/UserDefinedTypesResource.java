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
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Strings;
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
import io.stargate.web.models.Error;
import io.stargate.web.models.ResponseWrapper;
import io.stargate.web.models.UserDefinedTypeAdd;
import io.stargate.web.models.UserDefinedTypeField;
import io.stargate.web.models.UserDefinedTypeResponse;
import io.stargate.web.models.UserDefinedTypeUpdate;
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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.javatuples.Pair;

/**
 * Exposes REST Endpoint to work with Cassandra User Defined Types
 *
 * @see "https://cassandra.apache.org/doc/latest/cql/types.html"
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
  public static final String PATH_PARAM_KEYSPACE = "keyspaceName";

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
          AuthenticatedDB authenticatedDB =
              db.getRestDataStoreForToken(token, getAllHeaders(request));
          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);

          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "keyspace does not exists", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }
          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  null,
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          List<UserDefinedTypeResponse> udtResponses =
              authenticatedDB.getTypes(keyspaceName).stream()
                  .map(this::mapUdtAsResponse)
                  .collect(Collectors.toList());

          Object response = raw ? udtResponses : new ResponseWrapper<>(udtResponses);
          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get an user defined type (UDT) from its identifier",
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
          AuthenticatedDB authenticatedDB =
              db.getRestDataStoreForToken(token, getAllHeaders(request));
          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "keyspace does not exists", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          db.getAuthorizationService()
              .authorizeSchemaRead(
                  authenticatedDB.getAuthenticationSubject(),
                  Collections.singletonList(keyspaceName),
                  null,
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          UserDefinedTypeResponse udtResponse =
              mapUdtAsResponse(authenticatedDB.getType(keyspaceName, typeName));
          return Response.ok(
                  Converters.writeResponse(raw ? udtResponse : new ResponseWrapper<>(udtResponse)))
              .build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create an user defined type (UDT)",
      notes = "Add an user defined type (udt) in a specific keyspace.",
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
          AuthenticatedDB authenticatedDB =
              db.getRestDataStoreForToken(token, getAllHeaders(request));
          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);

          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "keyspace does not exists", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }
          String typeName = udtAdd.getName();
          if (Strings.isNullOrEmpty(typeName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "Type name must be provided", Response.Status.BAD_REQUEST.getStatusCode()))
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

          List<Column> columns;
          try {
            columns = getUdtColumns(keyspace, udtAdd.getFieldDefinitions());
          } catch (IllegalArgumentException | InvalidRequestException ex) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(new Error(ex.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          UserDefinedType udt =
              ImmutableUserDefinedType.builder()
                  .keyspace(keyspaceName)
                  .name(typeName)
                  .addColumns(columns.toArray(new Column[0]))
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

          return Response.status(Response.Status.CREATED)
              .entity(Converters.writeResponse(Collections.singletonMap("name", typeName)))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(
      value = "Delete an User Defined type (UDT)",
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
          AuthenticatedDB authenticatedDB =
              db.getRestDataStoreForToken(token, getAllHeaders(request));
          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
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
                  Scope.DROP,
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

  @Timed
  @PUT
  @ApiOperation(
      value = "Update an User Defined type (UDT)",
      notes = "Update an user defined type (UDT) adding or renaming fields.")
  @ApiResponses(
      value = {
        @ApiResponse(code = 204, message = "No Content"),
        @ApiResponse(code = 401, message = "Unauthorized", response = Error.class),
        @ApiResponse(code = 500, message = "Internal server error", response = Error.class)
      })
  public Response update(
      @ApiParam(
              value =
                  "The token returned from the authorization endpoint. Use this token in each request.",
              required = true)
          @HeaderParam(HEADER_TOKEN_AUTHENTICATION)
          String token,
      @ApiParam(value = "Name of the keyspace to use for the request.", required = true)
          @PathParam("keyspaceName")
          final String keyspaceName,
      @ApiParam(value = "", required = true) @NotNull final UserDefinedTypeUpdate udtUpdate,
      @Context HttpServletRequest request) {
    return RequestHandler.handle(
        () -> {
          AuthenticatedDB authenticatedDB =
              db.getRestDataStoreForToken(token, getAllHeaders(request));

          Keyspace keyspace = authenticatedDB.getDataStore().schema().keyspace(keyspaceName);
          if (keyspace == null) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "keyspace does not exists.", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          String typeName = udtUpdate.getName();
          if (Strings.isNullOrEmpty(typeName)) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "Type name must be provided.", Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          db.getAuthorizationService()
              .authorizeSchemaWrite(
                  authenticatedDB.getAuthenticationSubject(),
                  keyspaceName,
                  null,
                  Scope.ALTER,
                  SourceAPI.REST,
                  ResourceKind.TYPE);

          UserDefinedType udt =
              ImmutableUserDefinedType.builder().keyspace(keyspaceName).name(typeName).build();

          List<UserDefinedTypeField> fields = udtUpdate.getFieldDefinitions();
          if (fields != null && !fields.isEmpty()) {
            try {
              List<Column> columns = getUdtColumns(keyspace, fields);
              authenticatedDB
                  .getDataStore()
                  .queryBuilder()
                  .alter()
                  .type(keyspaceName, udt)
                  .addColumn(columns)
                  .build()
                  .execute(ConsistencyLevel.LOCAL_QUORUM)
                  .get();
            } catch (IllegalArgumentException | InvalidRequestException ex) {
              return Response.status(Response.Status.BAD_REQUEST)
                  .entity(new Error(ex.getMessage(), Response.Status.BAD_REQUEST.getStatusCode()))
                  .build();
            }
          } else if (udtUpdate.getRenameColumns() != null
              && !udtUpdate.getRenameColumns().isEmpty()) {
            List<Pair<String, String>> columns =
                udtUpdate.getRenameColumns().stream()
                    .map(r -> Pair.fromArray(new String[] {r.getFrom(), r.getTo()}))
                    .collect(Collectors.toList());
            authenticatedDB
                .getDataStore()
                .queryBuilder()
                .alter()
                .type(keyspaceName, udt)
                .renameColumn(columns)
                .build()
                .execute(ConsistencyLevel.LOCAL_QUORUM)
                .get();
          } else {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(
                    new Error(
                        "It must be informed either an add-type or rename-type.",
                        Response.Status.BAD_REQUEST.getStatusCode()))
                .build();
          }

          return Response.status(Response.Status.OK).build();
        });
  }

  private List<Column> getUdtColumns(Keyspace keyspace, List<UserDefinedTypeField> fields) {
    List<Column> columns = new ArrayList<>();
    for (UserDefinedTypeField colDef : fields) {
      String fieldName = colDef.getName();
      String typeDef = colDef.getTypeDefinition();
      if (Strings.isNullOrEmpty(fieldName) || Strings.isNullOrEmpty(typeDef)) {
        throw new IllegalArgumentException("Type name and definition must be provided.");
      }
      columns.add(
          Column.create(
              fieldName,
              Kind.Regular,
              Type.fromCqlDefinitionOf(keyspace, colDef.getTypeDefinition())));
    }

    if (columns.isEmpty()) {
      throw new IllegalArgumentException("There should be at least one field defined");
    }
    return columns;
  }

  private UserDefinedTypeResponse mapUdtAsResponse(UserDefinedType udt) {
    return new UserDefinedTypeResponse(
        udt.name(),
        udt.keyspace(),
        udt.columns().stream().map(this::mapUdtFieldDefinition).collect(Collectors.toList()));
  }

  private UserDefinedTypeField mapUdtFieldDefinition(Column col) {
    return new UserDefinedTypeField(
        col.name(), null == col.type() ? null : col.type().cqlDefinition());
  }
}
