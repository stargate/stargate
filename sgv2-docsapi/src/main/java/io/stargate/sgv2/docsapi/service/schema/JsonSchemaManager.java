package io.stargate.sgv2.docsapi.service.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingMessage;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.schema.query.JsonSchemaQueryProvider;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Json Schema manager provides basic operations to store, retrieve, and use the JSON schema for a
 * Documents collection.
 */
@ApplicationScoped
public class JsonSchemaManager {
  @Inject ObjectMapper objectMapper;

  @Inject JsonSchemaQueryProvider jsonSchemaQueryProvider;

  @Inject StargateRequestInfo requestInfo;

  private final JsonSchemaFactory jsonSchemaFactory = JsonSchemaFactory.byDefault();

  /**
   * Gets the JSON Schema for a given table.
   *
   * @param table
   * @return Uni containing the JsonNode representing the schema
   */
  public Uni<JsonNode> getJsonSchema(Uni<Schema.CqlTable> table) {
    // This properly handles authz based on which TableManager is provided
    return table
        .onItem()
        .ifNotNull()
        .transform(
            t -> {
              String comment = t.getOptionsMap().getOrDefault("comment", null);
              if (comment == null) {
                return null;
              }

              try {
                return objectMapper.readTree(comment);
              } catch (JsonProcessingException e) {
                return null;
              }
            });
  }

  /**
   * Assigns a JSON schema to a table.
   *
   * @param keyspace the keyspace of the table
   * @param table the table
   * @param schema the JSON schema to assign
   * @return a Uni with a success boolean
   */
  public Uni<QueryOuterClass.Response> attachJsonSchema(
      String keyspace, String table, JsonNode schema) {
    return Uni.createFrom()
        .item(() -> jsonSchemaFactory.getSyntaxValidator().validateSchema(schema))
        .flatMap(
            report -> {
              if (report.isSuccess()) {
                ObjectNode wrappedSchema = objectMapper.createObjectNode();
                wrappedSchema.set("schema", schema);
                StargateBridge bridge = requestInfo.getStargateBridge();
                return bridge.executeQuery(
                    jsonSchemaQueryProvider.attachSchemaQuery(
                        keyspace, table, wrappedSchema.toString()));
              } else {
                String msgs = "";
                Iterator<ProcessingMessage> it = report.iterator();
                while (it.hasNext()) {
                  ProcessingMessage msg = it.next();
                  msgs += String.format("[%s]: %s; ", msg.getLogLevel(), msg.getMessage());
                }
                Throwable failure =
                    new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_INVALID, msgs);
                return Uni.createFrom().failure(failure);
              }
            });
  }

  /**
   * Validates a JSON document against a given table's schema
   *
   * @param table the table that has a schema
   * @param document the document, as stringified JSON
   * @return a Uni with Boolean detailing whether or not the document complies with the schema.
   */
  public Uni<Boolean> validateJsonDocument(Uni<Schema.CqlTable> table, String document) {
    return table
        .onItem()
        .ifNotNull()
        .transform(
            t -> {
              String comment = t.getOptionsMap().getOrDefault("comment", null);
              if (comment == null) {
                // If there is no valid schema, then the document is valid
                return true;
              }
              JsonNode jsonSchema;
              try {
                jsonSchema = objectMapper.readTree(comment);
              } catch (JsonProcessingException e) {
                // If there is no valid schema, then the document is valid
                return true;
              }

              try {
                validate(jsonSchema, document);
              } catch (ProcessingException e) {
                return true;
              }
              return true;
            });
  }

  /**
   * Validates a JSON document against a given table's schema
   *
   * @param table the table that has a schema
   * @param document the document, as JsonNode
   * @return a Uni with Boolean detailing whether or not the document complies with the schema.
   */
  public Uni<Boolean> validateJsonDocument(Uni<Schema.CqlTable> table, JsonNode document) {
    return table
        .onItem()
        .ifNotNull()
        .transform(
            t -> {
              System.out.println("the value2: " + t.getOptionsMap().get("comment"));
              String comment = t.getOptionsMap().getOrDefault("comment", null);
              if (comment == null) {
                // If there is no valid schema, then the document is valid
                return true;
              }
              JsonNode jsonSchema;
              try {
                jsonSchema = objectMapper.readTree(comment);
              } catch (JsonProcessingException e) {
                // If there is no valid schema, then the document is valid
                return true;
              }

              try {
                validate(jsonSchema.get("schema"), document);
              } catch (ProcessingException e) {
                return true;
              }
              return true;
            });
  }

  private void validate(JsonNode schema, String value) throws ProcessingException {
    final JsonNode tree;
    try {
      tree = objectMapper.readTree(value);
    } catch (JsonProcessingException e) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Malformed JSON object found during read: " + e);
    }
    validate(schema, tree);
  }

  private void validate(JsonNode schema, JsonNode jsonValue) throws ProcessingException {
    ProcessingReport result = jsonSchemaFactory.getValidator().validate(schema, jsonValue);
    if (!result.isSuccess()) {
      List<String> messages = new ArrayList<>();
      result.forEach(msg -> messages.add(msg.getMessage()));
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Invalid JSON: " + messages);
    }
  }
}
