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
import io.dropwizard.util.Strings;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableUserDefinedType;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.web.models.Error;
import io.stargate.web.models.GetResponseWrapper;
import io.stargate.web.models.UdtAdd;
import io.stargate.web.models.udt.CQLType;
import io.stargate.web.models.udt.UdtInfo;
import io.stargate.web.models.udt.UdtType;
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
import javax.validation.constraints.NotNull;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

@Api(
    produces = MediaType.APPLICATION_JSON,
    consumes = MediaType.APPLICATION_JSON,
    tags = {"schemas"})
@Path("/v2/schemas/keyspaces/{keyspaceName}/udts")
@Produces(MediaType.APPLICATION_JSON)
public class UdtsResource {

  @Inject private Db db;

  @Timed
  @GET
  @ApiOperation(
      value = "Get all UDTs",
      notes = "Get all UDTs from a given keyspace",
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
                  ResourceKind.UDT);

          Map<String, Map<String, String>> response =
              authenticatedDB.getDataStore().schema().keyspace(keyspaceName).userDefinedTypes()
                  .stream()
                  .collect(Collectors.toMap(UserDefinedType::name, UdtsResource::convertColumnMap));

          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @GET
  @ApiOperation(
      value = "Get all UDTs",
      notes = "Get all UDTs from a given keyspace",
      response = GetResponseWrapper.class,
      responseContainer = "List")
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
                  ResourceKind.UDT);

          UserDefinedType udt =
              authenticatedDB
                  .getDataStore()
                  .schema()
                  .keyspace(keyspaceName)
                  .userDefinedType(typeName);

          Map<String, Map<String, String>> response = Maps.newHashMap();
          response.put(udt.name(), convertColumnMap(udt));

          return Response.status(Response.Status.OK)
              .entity(Converters.writeResponse(response))
              .build();
        });
  }

  @Timed
  @DELETE
  @ApiOperation(value = "Delete a udt", notes = "Delete a udt in the specified keyspace.")
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
              value =
                  "Determines whether to drop an udt if an udt with the name exists. Attempting to drop a non existing udt returns an error unless this option is true.")
          @PathParam("ifExists")
          final Boolean ifExists,
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
                  ResourceKind.UDT);

          authenticatedDB
              .getDataStore()
              .queryBuilder()
              .drop()
              .type(keyspaceName, UserDefinedType.reference(keyspaceName, typeName))
              .ifExists(ifExists != null && ifExists)
              .build()
              .execute(ConsistencyLevel.LOCAL_QUORUM)
              .get();

          return Response.status(Response.Status.NO_CONTENT).build();
        });
  }

  @Timed
  @POST
  @ApiOperation(
      value = "Create a UDT",
      notes = "Create a UDT in the given keyspace",
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
                  ResourceKind.UDT);

          List<Column> fields = fromUdtAdd(udtAdd);

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

          return Response.status(Response.Status.CREATED).build();
        });
  }

  static Map<String, String> convertColumnMap(UserDefinedType udt) {
    Map<String, String> map = new HashMap<>();
    for (Map.Entry<String, Column> e : udt.columnMap().entrySet()) {
      map.put(e.getKey(), e.getValue().toString());
    }
    return map;
  }

  private static List<Column> fromUdtAdd(UdtAdd udtAdd) {
    List<UdtType> fieldsList = udtAdd.getFields();
    if (fieldsList == null || fieldsList.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one field");
    }
    List<Column> fields = new ArrayList<>(fieldsList.size());

    for (UdtType field : fieldsList) {
      // TODO: TypeDef.name should be not null?
      if (Strings.isNullOrEmpty(field.getName())) {
        throw new IllegalArgumentException(
            "UDT definition should contain a 'name' field specifying the UDT field name.");
      }
      fields.add(Column.create(field.getName(), decodeType(field)));
    }
    return fields;
  }

  private static Column.ColumnType decodeType(UdtType udtType) {
    CQLType basic = udtType.getBasic();
    UdtInfo info = udtType.getInfo();
    List<UdtType> subTypes = info != null ? info.getSubTypes() : null;
    boolean frozen = info != null ? info.isFrozen() : false;

    switch (basic) {
      case INT:
        return Column.Type.Int;
      case INET:
        return Column.Type.Inet;
      case TIMEUUID:
        return Column.Type.Timeuuid;
      case TIMESTAMP:
        return Column.Type.Timestamp;
      case BIGINT:
        return Column.Type.Bigint;
      case TIME:
        return Column.Type.Time;
      case DURATION:
        return Column.Type.Duration;
      case VARINT:
        return Column.Type.Varint;
      case UUID:
        return Column.Type.Uuid;
      case BOOLEAN:
        return Column.Type.Boolean;
      case TINYINT:
        return Column.Type.Tinyint;
      case SMALLINT:
        return Column.Type.Smallint;
      case ASCII:
        return Column.Type.Ascii;
      case DECIMAL:
        return Column.Type.Decimal;
      case BLOB:
        return Column.Type.Blob;
      case VARCHAR:
      case TEXT:
        return Column.Type.Text;
      case DOUBLE:
        return Column.Type.Double;
      case COUNTER:
        return Column.Type.Counter;
      case DATE:
        return Column.Type.Date;
      case FLOAT:
        return Column.Type.Float;
      case LIST:
        if (info == null) {
          throw new IllegalArgumentException(
              "List cqlType should contain an 'info' field specifying the sub cqlType");
        }
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("List sub types should contain 1 item");
        }
        return Column.Type.List.of(decodeType(subTypes.get(0))).frozen(frozen);
      case SET:
        if (info == null) {
          throw new IllegalArgumentException(
              "Set cqlType should contain an 'info' field specifying the sub cqlType");
        }
        if (subTypes == null || subTypes.size() != 1) {
          throw new IllegalArgumentException("Set sub types should contain 1 item");
        }
        return Column.Type.Set.of(decodeType(subTypes.get(0))).frozen(frozen);
      case MAP:
        if (info == null) {
          throw new IllegalArgumentException(
              "Map cqlType should contain an 'info' field specifying the sub types");
        }
        if (subTypes == null || subTypes.size() != 2) {
          throw new IllegalArgumentException("Map sub types should contain 2 items");
        }
        return Column.Type.Map.of(decodeType(subTypes.get(0)), decodeType(subTypes.get(1)))
            .frozen(frozen);
      case UDT:
        if (info == null || Strings.isNullOrEmpty(info.getName())) {
          throw new IllegalArgumentException(
              "UDT cqlType should contain an 'info' field specifying the UDT name");
        }
        return UserDefinedType.reference(udtType.getName()).frozen(frozen);
      case TUPLE:
        if (info == null) {
          throw new IllegalArgumentException(
              "TUPLE cqlType should contain an 'info' field specifying the sub types");
        }
        if (subTypes == null || subTypes.isEmpty()) {
          throw new IllegalArgumentException("TUPLE cqlType should have at least one sub cqlType");
        }
        Column.ColumnType[] decodedSubTypes =
            subTypes.stream()
                .map(UdtsResource::decodeType)
                .collect(Collectors.toList())
                .toArray(new Column.ColumnType[0]);
        return Column.Type.Tuple.of(decodedSubTypes);
    }
    throw new RuntimeException(String.format("Data cqlType %s is not supported", basic));
  }
}
