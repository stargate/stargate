package io.stargate.sgv2.restapi.service.resources.schemas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.Column;
import io.stargate.sgv2.api.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.restapi.service.models.Sgv2NameResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2RESTResponse;
import io.stargate.sgv2.restapi.service.models.Sgv2UDT;
import io.stargate.sgv2.restapi.service.models.Sgv2UDTAddRequest;
import io.stargate.sgv2.restapi.service.models.Sgv2UDTUpdateRequest;
import io.stargate.sgv2.restapi.service.resources.RestResourceBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.RestResponse;

public class Sgv2UDTsResourceImpl extends RestResourceBase implements Sgv2UDTsResourceApi {
  @Override
  public Uni<RestResponse<Object>> findAllTypes(final String keyspaceName, final boolean raw) {
    QueryOuterClass.Query query =
        new QueryBuilder()
            .select()
            .column("type_name")
            .column("field_names")
            .column("field_types")
            .from("system_schema", "types")
            .where("keyspace_name", Predicate.EQ, Values.of(keyspaceName))
            .build();

    return executeQueryAsync(query)
        .map(response -> response.getResultSet())
        .map(
            rs -> {
              // two-part conversion: first from proto to JsonNode for easier traversability,
              // then from that to actual response we need:
              ArrayNode ksRows = convertRowsToArrayNode(rs);
              return jsonArray2Udts(keyspaceName, ksRows);
            })
        .map(udts -> raw ? udts : new Sgv2RESTResponse<>(udts))
        .map(result -> RestResponse.ok(result));
  }

  @Override
  public Uni<RestResponse<Object>> findTypeById(
      final String keyspaceName, final String typeName, final boolean raw) {
    QueryOuterClass.Query query =
        new QueryBuilder()
            .select()
            .column("type_name")
            .column("field_names")
            .column("field_types")
            .from("system_schema", "types")
            .where("keyspace_name", Predicate.EQ, Values.of(keyspaceName))
            .where("type_name", Predicate.EQ, Values.of(typeName))
            .build();

    return executeQueryAsync(query)
        .map(response -> response.getResultSet())
        .map(rs -> convertRowsToArrayNode(rs))
        .map(
            ksRows -> {
              // Must get one and only one response, verify
              switch (ksRows.size()) {
                case 0:
                  throw new WebApplicationException(
                      String.format(
                          "No definition found for UDT '%s' (keyspace '%s')",
                          typeName, keyspaceName),
                      Response.Status.NOT_FOUND);
                case 1:
                  Sgv2UDT udt = jsonArray2Udts(keyspaceName, ksRows).get(0);
                  final Object udtResult = raw ? udt : new Sgv2RESTResponse(udt);
                  return RestResponse.ok(udtResult);
                default:
                  throw new WebApplicationException(
                      String.format(
                          "Multiple definitions (%d) found for UDT '%s' (keyspace '%s')",
                          ksRows.size(), typeName, keyspaceName),
                      Response.Status.INTERNAL_SERVER_ERROR);
              }
            });
  }

  @Override
  public Uni<RestResponse<Sgv2NameResponse>> createType(
      final String keyspaceName, final String udtAddPayload) {
    Sgv2UDTAddRequest udtAdd;

    try {
      udtAdd = parseJsonAs(udtAddPayload, Sgv2UDTAddRequest.class);
    } catch (IOException e) {
      throw new WebApplicationException(
          "Invalid JSON payload: " + e.getMessage(), Response.Status.BAD_REQUEST);
    }
    final String typeName = udtAdd.name();
    if (typeName == null || typeName.isEmpty()) {
      throw new WebApplicationException("typeName must be provided", Response.Status.BAD_REQUEST);
    }

    QueryOuterClass.Query query =
        new QueryBuilder()
            .create()
            .type(keyspaceName, typeName)
            .ifNotExists(udtAdd.ifNotExists())
            .column(columns2columns(udtAdd.fields()))
            .parameters(PARAMETERS_FOR_LOCAL_QUORUM)
            .build();

    return executeQueryAsync(query)
        // For most failures pass and let default handler deal; but for specific case of
        // trying to create existing UDT without "if-not-exists", try to dig actual fail
        // message
        .map(any -> restResponseCreatedWithName(typeName))
        .onFailure(
            failure ->
                (failure instanceof StatusRuntimeException)
                    && ((StatusRuntimeException) failure)
                        .getStatus()
                        .getDescription()
                        .contains("already exists"))
        .transform(
            failure ->
                new WebApplicationException(
                    "Bad request: "
                        + ((StatusRuntimeException) failure).getStatus().getDescription(),
                    Response.Status.BAD_REQUEST));
  }

  @Override
  public Uni<RestResponse<Void>> updateType(
      final String keyspaceName, final Sgv2UDTUpdateRequest udtUpdate) {
    final String typeName = udtUpdate.name();

    List<Sgv2UDT.UDTField> addFields = udtUpdate.addFields();
    List<Sgv2UDTUpdateRequest.FieldRename> renameFields = udtUpdate.renameFields();

    final boolean hasAddFields = (addFields != null && !addFields.isEmpty());
    final boolean hasRenameFields = (renameFields != null && !renameFields.isEmpty());

    if (!hasAddFields && !hasRenameFields) {
      throw new WebApplicationException(
          "addFields and/or renameFields is required to update an UDT",
          Response.Status.BAD_REQUEST);
    }

    final QueryOuterClass.Query addQuery =
        hasAddFields
            ? new QueryBuilder()
                .alter()
                .type(keyspaceName, typeName)
                .addColumn(columns2columns(addFields))
                .build()
            : null;

    if (hasRenameFields) {
      final Map<String, String> columnRenames =
          renameFields.stream()
              .collect(
                  Collectors.toMap(
                      Sgv2UDTUpdateRequest.FieldRename::from,
                      Sgv2UDTUpdateRequest.FieldRename::to));
      final QueryOuterClass.Query renameQuery =
          new QueryBuilder()
              .alter()
              .type(keyspaceName, typeName)
              .renameColumn(columnRenames)
              .build();
      if (addQuery != null) {
        return executeQueryAsync(addQuery)
            .chain(any -> executeQueryAsync(renameQuery))
            .map(any -> RestResponse.ok());
      }
      return executeQueryAsync(renameQuery).map(any -> RestResponse.ok());
    }

    // Must still have add-columns
    return executeQueryAsync(addQuery).map(any -> RestResponse.ok());
  }

  @Override
  public Uni<RestResponse<Void>> deleteType(final String keyspaceName, final String typeName) {
    return executeQueryAsync(
            new QueryBuilder().drop().type(keyspaceName, typeName).ifExists().build())
        .map(any -> RestResponse.noContent());
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper methods
  /////////////////////////////////////////////////////////////////////////
   */

  private List<Column> columns2columns(List<Sgv2UDT.UDTField> fields) {
    List<Column> result = new ArrayList<>();
    for (Sgv2UDT.UDTField colDef : fields) {
      String columnName = colDef.name();
      String typeDef = colDef.typeDefinition();
      if (isStringEmpty(columnName) || isStringEmpty(typeDef)) {
        throw new WebApplicationException(
            "Field 'name' and 'typeDefinition' must be provided", Response.Status.BAD_REQUEST);
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
}
