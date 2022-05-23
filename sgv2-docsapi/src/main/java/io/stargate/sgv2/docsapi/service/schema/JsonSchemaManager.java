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

  public Uni<JsonNode> getJsonSchema(TableManager tableManager, String keyspace, String table) {
    // This properly handles authz based on which TableManager is provided
    return tableManager
        .getValidCollectionTable(keyspace, table)
        .map(
            t -> {
              String comment = t.getOptionsMap().getOrDefault("comment", "");
              try {
                return objectMapper.readTree(comment);
              } catch (JsonProcessingException e) {
                return null;
              }
            });
  }

  public Uni<QueryOuterClass.Response> attachJsonSchema(
      String keyspace, String table, JsonNode schema) {
    ProcessingReport report = jsonSchemaFactory.getSyntaxValidator().validateSchema(schema);
    if (report.isSuccess()) {
      ObjectNode wrappedSchema = objectMapper.createObjectNode();
      wrappedSchema.set("schema", schema);
      StargateBridge bridge = requestInfo.getStargateBridge();
      return bridge.executeQuery(
          jsonSchemaQueryProvider.attachSchemaQuery(keyspace, table, wrappedSchema.toString()));
    } else {
      String msgs = "";
      Iterator<ProcessingMessage> it = report.iterator();
      while (it.hasNext()) {
        ProcessingMessage msg = it.next();
        msgs += String.format("[%s]: %s; ", msg.getLogLevel(), msg.getMessage());
      }
      throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_JSON_SCHEMA_INVALID, msgs);
    }
  }

  public Uni<Boolean> validateJsonSchema(
      TableManager tableManager, String keyspace, String table, JsonNode document) {
    return getJsonSchema(tableManager, keyspace, table)
        .map(
            jsonSchema -> {
              ProcessingReport result = null;
              try {
                result = jsonSchemaFactory.getValidator().validate(jsonSchema, document);
              } catch (ProcessingException e) {
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Invalid JSON: " + e.getMessage());
              }

              if (!result.isSuccess()) {
                List<String> messages = new ArrayList<>();
                result.forEach(msg -> messages.add(msg.getMessage()));
                throw new ErrorCodeRuntimeException(
                    ErrorCode.DOCS_API_INVALID_JSON_VALUE, "Invalid JSON: " + messages.toString());
              }

              return true;
            });
  }
}
