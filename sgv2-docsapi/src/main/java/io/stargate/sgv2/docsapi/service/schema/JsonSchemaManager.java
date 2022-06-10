package io.stargate.sgv2.docsapi.service.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.docsapi.api.common.StargateRequestInfo;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.exception.ErrorCodeRuntimeException;
import io.stargate.sgv2.docsapi.service.schema.query.JsonSchemaQueryProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Json Schema manager provides basic operations to store, retrieve, and use the JSON schema for a
 * Documents collection.
 */
@ApplicationScoped
public class JsonSchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(JsonSchemaManager.class);

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
              if (comment == null || comment.isEmpty()) {
                return null;
              }

              try {
                return objectMapper.readTree(comment);
              } catch (JsonProcessingException e) {
                logger.warn("Document table has comment, but it's not a valid JSON.");
                return null;
              }
            });
  }

  /**
   * Assigns a JSON schema to a table.
   *
   * @param namespace the namespace of the collection
   * @param collection the collection to attach JSON schema to
   * @param schema the JSON schema to assign
   * @return a Uni with a success boolean
   */
  public Uni<JsonNode> attachJsonSchema(
      String namespace, Uni<Schema.CqlTable> collection, JsonNode schema) {
    return collection.flatMap(
        c -> {
          ProcessingReport report = jsonSchemaFactory.getSyntaxValidator().validateSchema(schema);
          if (report.isSuccess()) {
            StargateBridge bridge = requestInfo.getStargateBridge();
            return bridge
                .executeQuery(
                    jsonSchemaQueryProvider.attachSchemaQuery(
                        namespace, c.getName(), schema.toString()))
                .map(r -> schema);
          } else {
            String msgs =
                StreamSupport.stream(report.spliterator(), false)
                    .map(msg -> String.format("[%s]: %s; ", msg.getLogLevel(), msg.getMessage()))
                    .collect(Collectors.joining());
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
   * @param document the document, as JsonNode
   * @return a Uni with Boolean detailing whether or not the document complies with the schema.
   */
  public Uni<Boolean> validateJsonDocument(Uni<Schema.CqlTable> table, JsonNode document) {
    return getJsonSchema(table)
        .onItem()
        .transform(
            jsonSchema -> {
              if (jsonSchema == null) {
                // If there is no valid JSON schema, then the document is valid
                return true;
              }

              try {
                validate(jsonSchema, document);
              } catch (ProcessingException e) {
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_JSON_SCHEMA_PROCESSING_FAILED);
              }
              return true;
            });
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
