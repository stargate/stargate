package io.stargate.sgv2.docsapi.service.function.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.service.function.BuiltInFunction;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Set function implementation. Takes an object with paths in the document to set to a value, and
 * sets them all in a batch. Supports use of dot-notation to reach nested paths without touching
 * other data in nested objects and arrays.
 */
@ApplicationScoped
public class SetFunction implements BuiltInFunction {
  @Inject WriteDocumentsService writeDocumentsService;

  @Inject ObjectMapper mapper;

  /** {@inheritDoc} */
  @Override
  public String getOperation() {
    return "$set";
  }

  /** {@inheritDoc} */
  @Override
  public Uni<JsonNode> execute(Uni<Schema.CqlTable> table, BuiltInFunction.Data data) {
    return writeDocumentsService
        .setPathsOnDocument(
            table,
            data.namespace(),
            data.collection(),
            data.documentId(),
            data.documentPath(),
            data.input(),
            true,
            data.executionContext())
        .map(__ -> mapper.nullNode());
  }
}
