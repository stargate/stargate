package io.stargate.sgv2.restsvc.resources.schemas;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.grpc.StatusRuntimeException;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restsvc.models.Sgv2RESTResponse;
import io.stargate.sgv2.restsvc.models.Sgv2UDT;
import io.stargate.sgv2.restsvc.models.Sgv2UDTAddRequest;
import io.stargate.sgv2.restsvc.models.Sgv2UDTUpdateRequest;
import io.stargate.sgv2.restsvc.resources.CreateGrpcStub;
import io.stargate.sgv2.restsvc.resources.ResourceBase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Path("/v2/schemas/keyspaces/{keyspaceName}/types")
@Singleton
@CreateGrpcStub
public class Sgv2UDTsResourceImpl extends ResourceBase implements Sgv2UDTsResourceApi {
  @Override
  public Response findAll(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final boolean raw,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);

    String cql =
        new QueryBuilder()
            .select()
            .column("type_name")
            .column("field_names")
            .column("field_types")
            .from("system_schema", "types")
            .where("keyspace_name", Predicate.EQ, keyspaceName)
            .build();

    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();

    // two-part conversion: first from proto to JsonNode for easier traversability,
    // then from that to actual response we need:
    ArrayNode ksRows = convertRowsToArrayNode(rs);
    List<Sgv2UDT> udts = jsonArray2Udts(keyspaceName, ksRows);
    final Object payload = raw ? udts : new Sgv2RESTResponse(udts);
    return Response.status(Response.Status.OK).entity(payload).build();
  }

  @Override
  public Response findById(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String typeName,
      final boolean raw,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);
    requireNonEmptyTypename(typeName);

    String cql =
        new QueryBuilder()
            .select()
            .column("type_name")
            .column("field_names")
            .column("field_types")
            .from("system_schema", "types")
            .where("keyspace_name", Predicate.EQ, keyspaceName)
            .where("type_name", Predicate.EQ, typeName)
            .build();

    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    QueryOuterClass.Response grpcResponse = blockingStub.executeQuery(query);

    final QueryOuterClass.ResultSet rs = grpcResponse.getResultSet();

    // two-part conversion: first from proto to JsonNode for easier traversability,
    // then from that to actual response we need:
    ArrayNode ksRows = convertRowsToArrayNode(rs);

    // Must get one and only one response, verify
    switch (ksRows.size()) {
      case 0:
        return Response.status(Response.Status.NOT_FOUND).build();
      case 1:
        break; // correct choice :)
      default:
        return restServiceError(
            Response.Status.INTERNAL_SERVER_ERROR,
            String.format(
                "Multiple definitions (%d) found for UDT '%s' (keyspace '%s')",
                ksRows.size(), typeName, keyspaceName));
    }

    Sgv2UDT udt = jsonArray2Udts(keyspaceName, ksRows).get(0);
    final Object payload = raw ? udt : new Sgv2RESTResponse(udt);
    return Response.status(Response.Status.OK).entity(payload).build();
  }

  @Override
  public Response createType(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final Sgv2UDTAddRequest udtAdd,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);
    final String typeName = udtAdd.getName();
    requireNonEmptyTypename(typeName);

    final String cql =
        new QueryBuilder()
            .create()
            .type(keyspaceName, typeName)
            .ifNotExists(udtAdd.getIfNotExists())
            .column(columns2columns(udtAdd.getFields()))
            .build();
    try {
      blockingStub.executeQuery(
          QueryOuterClass.Query.newBuilder()
              .setParameters(parametersForLocalQuorum())
              .setCql(cql)
              .build());
    } catch (StatusRuntimeException grpcE) {
      // For most failures pass and let default handler deal; but for specific case of
      // trying to create existing UDT without "if-not-exists", try to dig actual fail
      // message
      switch (grpcE.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          final String desc = grpcE.getStatus().getDescription();
          if (desc.contains("already exists")) {
            return restServiceError(Response.Status.BAD_REQUEST, "Bad request: " + desc);
          }
      }
      // otherwise just pass it along
      throw grpcE;
    }

    return Response.status(Response.Status.CREATED)
        .entity(Collections.singletonMap("name", typeName))
        .build();
  }

  @Override
  public Response delete(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final String typeName,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);
    requireNonEmptyTypename(typeName);

    String cql =
        new QueryBuilder()
            .drop()
            .type(keyspaceName, typeName)
            // 15-Dec-2021, tatu: Why no "ifExists" option? SGv1 had none which
            //    seems inconsistent; would be good for idempotency
            // .ifExists()
            .build();
    QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
    /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    return Response.status(Response.Status.NO_CONTENT).build();
  }

  @Override
  public Response update(
      final StargateGrpc.StargateBlockingStub blockingStub,
      final String keyspaceName,
      final Sgv2UDTUpdateRequest udtUpdate,
      final HttpServletRequest request) {
    requireNonEmptyKeyspace(keyspaceName);
    final String typeName = udtUpdate.getName();
    requireNonEmptyTypename(typeName);

    List<Sgv2UDT.UDTField> addFields = udtUpdate.getAddFields();
    List<Sgv2UDTUpdateRequest.FieldRename> renameFields = udtUpdate.getRenameFields();

    if ((addFields == null || addFields.isEmpty())
        && (renameFields == null || renameFields.isEmpty())) {
      return restServiceError(
          Response.Status.BAD_REQUEST,
          "addFields and/or renameFields is required to update an UDT");
    }

    if (addFields != null && !addFields.isEmpty()) {
      List<Column> columns = columns2columns(addFields);
      final String cql =
          new QueryBuilder().alter().type(keyspaceName, typeName).addColumn(columns).build();
      QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
      /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    }

    if (renameFields != null && !renameFields.isEmpty()) {
      Map<String, String> columnRenames =
          renameFields.stream()
              .collect(
                  Collectors.toMap(
                      Sgv2UDTUpdateRequest.FieldRename::getFrom,
                      Sgv2UDTUpdateRequest.FieldRename::getTo));

      final String cql =
          new QueryBuilder()
              .alter()
              .type(keyspaceName, typeName)
              .renameColumn(columnRenames)
              .build();
      QueryOuterClass.Query query = QueryOuterClass.Query.newBuilder().setCql(cql).build();
      /*QueryOuterClass.Response grpcResponse =*/ blockingStub.executeQuery(query);
    }

    return Response.status(Response.Status.OK).build();
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private List<Column> columns2columns(List<Sgv2UDT.UDTField> fields) {
    List<Column> result = new ArrayList<>();
    for (Sgv2UDT.UDTField colDef : fields) {
      String columnName = colDef.getName();
      String typeDef = colDef.getTypeDefinition();
      if (isStringEmpty(columnName) || isStringEmpty(typeDef)) {
        throw new WebApplicationException(
            "Field name and type definition must be provided", Response.Status.BAD_REQUEST);
      }
      result.add(
          ImmutableColumn.builder()
              .name(columnName)
              .kind(Column.Kind.REGULAR)
              .type(typeDef)
              .build());
    }
    if (result.isEmpty()) {
      throw new WebApplicationException(
          "There should be at least one field defined", Response.Status.BAD_REQUEST);
    }
    return result;
  }

  private List<Sgv2UDT> jsonArray2Udts(String keyspaceName, ArrayNode udtsJson) {
    List<Sgv2UDT> result = new ArrayList<>(udtsJson.size());
    for (JsonNode udtJson : udtsJson) {
      result.add(jsonObject2Udt(keyspaceName, udtJson));
    }
    return result;
  }

  private Sgv2UDT jsonObject2Udt(String keyspace, JsonNode udtsJson) {
    if (!udtsJson.isObject()) {
      throw new IllegalArgumentException(
          "UDT JSON Representation must be JSON Object, was: " + udtsJson);
    }
    final String typeName = nonEmptyStringProperty(udtsJson, "type_name");
    final JsonNode fieldNames = nonEmptyArrayProperty(udtsJson, "field_names");
    final JsonNode fieldTypes = nonEmptyArrayProperty(udtsJson, "field_types");
    final int fieldNameCount = fieldNames.size();
    if (fieldNameCount != fieldTypes.size()) {
      throw new IllegalArgumentException(
          String.format(
              "UDT JSON arrays for 'field_names' and 'field_types' must have same number of entries, got %d vs %d: %s",
              fieldNameCount, fieldTypes.size(), udtsJson));
    }
    List<Sgv2UDT.UDTField> fields = new ArrayList<>(fieldNameCount);
    for (int i = 0; i < fieldNameCount; ++i) {
      fields.add(new Sgv2UDT.UDTField(fieldNames.path(i).asText(), fieldTypes.path(i).asText()));
    }
    return new Sgv2UDT(typeName, keyspace, fields);
  }

  private static String nonEmptyStringProperty(JsonNode object, String propertyName) {
    String value = object.path(propertyName).asText();
    if (isStringEmpty(value)) {
      throw new IllegalArgumentException(
          String.format(
              "UDT JSON must have non-empty String property '%s', does not: %s",
              propertyName, object));
    }
    return value;
  }

  private static JsonNode nonEmptyArrayProperty(JsonNode object, String propertyName) {
    JsonNode value = object.path(propertyName); // may return MissingNode but not null
    if (!value.isArray() || value.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "UDT JSON must have non-empty Array property '%s', does not: %s",
              propertyName, object));
    }
    return value;
  }

  private static void requireNonEmptyTypename(String typeName) {
    if (isStringEmpty(typeName)) {
      throw new WebApplicationException("Type name must be provided", Response.Status.BAD_REQUEST);
    }
  }
}
