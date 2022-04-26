package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.JsonSchemaResponse;
import io.stargate.web.docsapi.service.util.ImmutableKeyspaceAndTable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;

/** Service that handles attaching and fetching JSON schemas to/from Documents API collections. */
public class JsonSchemaHandler {
  private final ObjectMapper mapper;
  private final JsonSchemaFactory schemaFactory = JsonSchemaFactory.byDefault();
  private final ConcurrentHashMap<ImmutableKeyspaceAndTable, JsonNode> schemasPerCollection =
      new ConcurrentHashMap<>();
  private Schema lastCheckedSchema;

  @Inject
  public JsonSchemaHandler(ObjectMapper mapper) {
    if (mapper == null) {
      throw new IllegalStateException("JsonSchemaHandler requires a non-null ObjectMapper");
    }
    this.mapper = mapper;
  }

  private void clearCacheOnSchemaChange(DocumentDB db) {
    if (!db.schema().equals(lastCheckedSchema)) {
      schemasPerCollection.clear();
      this.lastCheckedSchema = db.schema();
    }
  }

  private JsonSchemaResponse reportToResponse(JsonNode schema, ProcessingReport report) {
    JsonSchemaResponse resp = new JsonSchemaResponse(schema);
    report.forEach(msg -> resp.addMessage(msg.getLogLevel(), msg.getMessage()));
    return resp;
  }

  public JsonSchemaResponse attachSchemaToCollection(
      DocumentDB db, String namespace, String collection, JsonNode schema) {
    ProcessingReport report = schemaFactory.getSyntaxValidator().validateSchema(schema);
    JsonSchemaResponse resp = reportToResponse(schema, report);
    if (report.isSuccess()) {
      ObjectNode wrappedSchema = mapper.createObjectNode();
      wrappedSchema.set("schema", schema);
      writeSchemaToCollection(db, namespace, collection, wrappedSchema.toString());
      ImmutableKeyspaceAndTable info =
          ImmutableKeyspaceAndTable.builder().keyspace(namespace).table(collection).build();
      schemasPerCollection.remove(info);
      return resp;
    } else {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_INVALID);
    }
  }

  private String getRawJsonSchemaForCollection(DocumentDB db, String namespace, String collection) {
    String comment = db.getTable(namespace, collection).comment();
    return comment.isEmpty() ? null : comment;
  }

  public JsonSchemaResponse getJsonSchemaForCollection(
      DocumentDB db, String namespace, String collection) {
    String schemaData = getRawJsonSchemaForCollection(db, namespace, collection);
    if (null == schemaData) {
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_DOES_NOT_EXIST);
    }

    try {
      return new JsonSchemaResponse(mapper.readTree(schemaData).requiredAt("/schema"));
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * Attaches a comment to the given C* table that represents a JSON schema.
   *
   * @param namespace The namespace containing the collection to be altered
   * @param collection The collection to be altered by adding the JSON schema as a comment
   * @param schemaData The JSON schema to add as a comment to the table
   */
  private void writeSchemaToCollection(
      DocumentDB db, String namespace, String collection, String schemaData) {
    db.writeJsonSchemaToCollection(namespace, collection, schemaData);
  }

  public JsonNode getCachedJsonSchema(DocumentDB db, String namespace, String collection) {
    ImmutableKeyspaceAndTable info =
        ImmutableKeyspaceAndTable.builder().keyspace(namespace).table(collection).build();
    clearCacheOnSchemaChange(db);
    return schemasPerCollection.computeIfAbsent(
        info,
        ksAndTable -> {
          String schemaStr =
              getRawJsonSchemaForCollection(db, ksAndTable.getKeyspace(), ksAndTable.getTable());
          if (schemaStr == null) {
            return null;
          }
          try {
            return mapper.readTree(schemaStr).requiredAt("/schema");
          } catch (JsonProcessingException e) {
            return null;
          }
        });
  }

  public void validate(JsonNode schema, String value) throws ProcessingException {
    final JsonNode tree;
    try {
      tree = mapper.readTree(value);
    } catch (JsonProcessingException e) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Malformed JSON object found during read: "+e);
    }
    validate(schema, tree);
  }

  public void validate(JsonNode schema, JsonNode jsonValue) throws ProcessingException {
    ProcessingReport result = schemaFactory.getValidator().validate(schema, jsonValue);
    if (!result.isSuccess()) {
      List<String> messages = new ArrayList<>();
      result.forEach(msg -> messages.add(msg.getMessage()));
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Invalid JSON: " + messages);
    }
  }
}
