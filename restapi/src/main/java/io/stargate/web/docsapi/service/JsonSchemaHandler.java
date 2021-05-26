package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.JsonSchemaResponse;
import io.stargate.web.docsapi.service.util.ImmutableKeyspaceAndTable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

/** Service that handles attaching and fetching JSON schemas to/from Documents API collections. */
public class JsonSchemaHandler {
  private ObjectMapper mapper;
  private final JsonSchemaFactory schemaFactory = JsonSchemaFactory.byDefault();
  private final ConcurrentHashMap<ImmutableKeyspaceAndTable, JsonNode> schemasPerCollection =
      new ConcurrentHashMap<>();

  @Inject
  public JsonSchemaHandler(ObjectMapper mapper) {
    if (mapper == null) {
      throw new IllegalStateException("JsonSchemaHandler requires a non-null ObjectMapper");
    }
    this.mapper = mapper;
  }

  private JsonSchemaResponse reportToResponse(JsonNode schema, ProcessingReport report) {
    JsonSchemaResponse resp = new JsonSchemaResponse(schema);
    report.forEach(
        msg -> {
          resp.addMessage(msg.getLogLevel(), msg.getMessage());
        });
    return resp;
  }

  public JsonSchemaResponse attachSchemaToCollection(
      DocumentDB db, String namespace, String collection, JsonNode schema) {
    ProcessingReport report = schemaFactory.getSyntaxValidator().validateSchema(schema);
    JsonSchemaResponse resp = reportToResponse(schema, report);
    if (report.isSuccess()) {
      writeSchemaToCollection(db, namespace, collection, schema.toString());
      ImmutableKeyspaceAndTable info =
          ImmutableKeyspaceAndTable.builder().keyspace(namespace).table(collection).build();
      schemasPerCollection.put(info, schema);
      return resp;
    } else {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_INVALID);
    }
  }

  private CompletableFuture<String> getRawJsonSchemaForCollection(
      DocumentDB db, String namespace, String collection) {
    BuiltQuery q =
        db.builder()
            .select()
            .column("comment")
            .from("system_schema", "tables")
            .where("keyspace_name", Predicate.EQ, namespace)
            .where("table_name", Predicate.EQ, collection)
            .build();
    CompletableFuture<ResultSet> result = q.execute();
    return result.thenApply(
        rs -> {
          List<Row> rows = rs.currentPageRows();
          if (rows.size() > 0) {
            String comment = rows.get(0).getString("comment");
            return comment.isEmpty() ? null : comment;
          } else {
            return null;
          }
        });
  }

  public CompletableFuture<JsonSchemaResponse> getJsonSchemaForCollection(
      DocumentDB db, String namespace, String collection) {
    return getRawJsonSchemaForCollection(db, namespace, collection)
        .thenApply(
            schemaData -> {
              try {
                return new JsonSchemaResponse(mapper.readTree(schemaData));
              } catch (JsonProcessingException e) {
                return null;
              }
            });
  }

  /**
   * Attaches a comment to the given C* table that represents a JSON schema.
   *
   * @param namespace
   * @param collection
   * @param schemaData
   */
  private void writeSchemaToCollection(
      DocumentDB db, String namespace, String collection, String schemaData) {
    db.builder().alter().table(namespace, collection).withComment(schemaData).build().execute();
  }

  public JsonNode getCachedJsonSchema(DocumentDB db, String namespace, String collection) {
    ImmutableKeyspaceAndTable info =
        ImmutableKeyspaceAndTable.builder().keyspace(namespace).table(collection).build();
    return schemasPerCollection.computeIfAbsent(
        info,
        ksAndTable ->
            getRawJsonSchemaForCollection(db, ksAndTable.getKeyspace(), ksAndTable.getTable())
                .thenApply(
                    schemaStr -> {
                      if (schemaStr == null) {
                        return null;
                      }
                      try {
                        return mapper.readTree(schemaStr);
                      } catch (JsonProcessingException e) {
                        return null;
                      }
                    })
                .join());
  }

  public void validate(JsonNode schema, String value)
      throws ProcessingException, JsonProcessingException {
    JsonNode jsonValue = mapper.readTree(value);
    ProcessingReport result = schemaFactory.getValidator().validate(schema, jsonValue);
    if (!result.isSuccess()) {
      List<String> messages = new ArrayList<>();
      result.forEach(msg -> messages.add(msg.getMessage()));
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Invalid JSON: " + messages.toString());
    }
  }
}
