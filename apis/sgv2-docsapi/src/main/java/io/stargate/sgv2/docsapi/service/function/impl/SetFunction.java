package io.stargate.sgv2.docsapi.service.function.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.v2.model.dto.DocumentResponseWrapper;
import io.stargate.sgv2.docsapi.service.function.BuiltInFunction;
import io.stargate.sgv2.docsapi.service.write.WriteDocumentsService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

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
    // convert data input into rows
    if (!data.input().isObject()) {
      return null;
    }

    ObjectNode objInput = (ObjectNode) data.input();
    Iterator<Map.Entry<String, JsonNode>> iter = objInput.fields();
    List<Uni<DocumentResponseWrapper<Void>>> setResults = new ArrayList<>();
    while (iter.hasNext()) {
      Map.Entry<String, JsonNode> field = iter.next();
      String dottedPath = field.getKey();
      JsonNode subDoc = field.getValue();
      // Turn the dottedPath into expected path format
      String[] pathSegments = dottedPath.split("\\.");
      List<String> subPath = Arrays.asList(pathSegments);
      setResults.add(
          writeDocumentsService.updateSubDocument(
              table,
              data.namespace(),
              data.collection(),
              data.documentId(),
              subPath,
              subDoc,
              false,
              data.executionContext()));
    }
    return Multi.createFrom()
        .iterable(setResults)
        .collect()
        .asList()
        .onItem()
        .transform(__ -> mapper.createObjectNode());
  }
}
