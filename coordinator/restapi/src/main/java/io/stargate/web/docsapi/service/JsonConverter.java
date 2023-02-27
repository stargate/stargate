package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.web.docsapi.service.json.DeadLeafCollector;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeaf;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeafCollector;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import io.stargate.web.docsapi.service.util.DocsApiUtils;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

public class JsonConverter {
  private final ObjectMapper mapper;
  private final DocsApiConfiguration docsApiConfiguration;

  @Inject
  public JsonConverter(ObjectMapper mapper, DocsApiConfiguration docsApiConfiguration) {
    if (mapper == null) {
      throw new IllegalStateException("JsonConverter requires a non-null ObjectMapper");
    }
    this.mapper = mapper;
    this.docsApiConfiguration = docsApiConfiguration;
  }

  public JsonNode convertToJsonDoc(
      List<Row> rows, boolean writeAllPathsAsObjects, boolean numericBooleans) {
    return convertToJsonDoc(
        rows, ImmutableDeadLeafCollector.of(), writeAllPathsAsObjects, numericBooleans);
  }

  public JsonNode convertToJsonDoc(
      List<Row> rows,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    int maxDepth = docsApiConfiguration.getMaxDepth();
    return convertToJsonDoc(rows, collector, writeAllPathsAsObjects, numericBooleans, maxDepth);
  }

  /**
   * Takes a List of rows from a documents table, iterating over it and constructing a JSON object
   * (or array) that is represented by those rows. Note: Because the system avoids
   * read-before-write, it is possible for the same "path" in a document to contain two different
   * sets of data from two different versions of the document. When this is encountered during the
   * conversion, the version that has the latest writetime is accepted, and the older version is
   * collected in the DeadLeafCollector to be purged in the background.
   *
   * @param rows Rows from the database
   * @param collector a DeadLeafCollector implementation, to handle deletion of old and conflicting
   *     data
   * @param writeAllPathsAsObjects Instead of writing arrays such as [1, 2], write an object such as
   *     {"0": 1, "1": 2}
   * @param numericBooleans If these rows do not support boolean values and are using tinyint
   *     instead
   * @param maxDepth The rows' max depth (i.e. the highest pN value)
   * @return the JsonNode represented by converting the @param rows
   */
  private JsonNode convertToJsonDoc(
      List<Row> rows,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans,
      int maxDepth) {
    JsonNode doc = mapper.createObjectNode();
    Map<String, Long> pathWriteTimes = new HashMap<>();
    if (rows.isEmpty()) {
      return doc;
    }
    Column writeTimeCol = Column.reference("writetime(leaf)");

    for (Row row : rows) {
      Long rowWriteTime = row.getLong(writeTimeCol.name());
      String rowLeaf = row.getString("leaf");
      if (rowLeaf.equals(DocsApiConstants.ROOT_DOC_MARKER)) {
        continue;
      }

      String leaf = null;
      JsonNode parentRef = null;
      JsonNode ref = doc;

      String parentPath = "$";

      for (int i = 0; i < maxDepth; i++) {
        String p = row.getString("p" + i);
        String nextP = i < maxDepth - 1 ? row.getString("p" + (i + 1)) : "";
        boolean endOfPath = nextP.equals("");
        boolean isArray = p.startsWith("[");
        boolean nextIsArray = nextP.startsWith("[");

        if (isArray) {
          // This removes leading zeros if applicable
          p = "[" + Integer.parseInt(p.substring(1, p.length() - 1)) + "]";
        }

        boolean shouldWrite =
            !pathWriteTimes.containsKey(parentPath)
                || pathWriteTimes.get(parentPath) <= rowWriteTime;

        if (!shouldWrite) {
          markFullPathAsDead(parentPath, p, collector);
          break;
        }

        if (endOfPath) {
          boolean shouldBeArray = isArray && !ref.isArray() && !writeAllPathsAsObjects;
          if (i == 0 && shouldBeArray) {
            doc = mapper.createArrayNode();
            ref = doc;
            pathWriteTimes.put(parentPath, rowWriteTime);
          } else if (i != 0 && shouldBeArray) {
            markObjectAtPathAsDead(ref, parentPath, collector);
            ref = changeCurrentNodeToArray(row, parentRef, i);
            pathWriteTimes.put(parentPath, rowWriteTime);
          } else if (i != 0 && !isArray && !ref.isObject()) {
            markArrayAtPathAsDead(ref, parentPath, collector);
            ref = changeCurrentNodeToObject(row, parentRef, i, writeAllPathsAsObjects);
            pathWriteTimes.put(parentPath, rowWriteTime);
          }
          leaf = p;
          break;
        }

        JsonNode childRef;

        if (isArray && !writeAllPathsAsObjects) {
          int index = Integer.parseInt(p.substring(1, p.length() - 1));

          boolean shouldBeArray = isArray && !ref.isArray();
          if (i == 0 && shouldBeArray) {
            doc = mapper.createArrayNode();
            ref = doc;
          } else if (shouldBeArray) {
            markObjectAtPathAsDead(ref, parentPath, collector);
            ref = changeCurrentNodeToArray(row, parentRef, i);
            pathWriteTimes.put(parentPath, rowWriteTime);
          }

          ArrayNode arrayRef = (ArrayNode) ref;

          int currentSize = arrayRef.size();
          for (int k = currentSize; k < index; k++) arrayRef.addNull();

          if (currentSize <= index) {
            childRef = nextIsArray ? mapper.createArrayNode() : mapper.createObjectNode();
            arrayRef.add(childRef);
          } else {
            childRef = arrayRef.get(index);

            // Replace null from above (out of order)
            if (childRef.isNull()) {
              childRef = nextIsArray ? mapper.createArrayNode() : mapper.createObjectNode();
            }

            arrayRef.set(index, childRef);
          }
        } else {
          childRef = ref.get(p);
          if (childRef == null) {
            childRef =
                nextIsArray && !writeAllPathsAsObjects
                    ? mapper.createArrayNode()
                    : mapper.createObjectNode();

            if (!ref.isObject()) {
              markArrayAtPathAsDead(ref, parentPath, collector);
              ref = changeCurrentNodeToObject(row, parentRef, i, writeAllPathsAsObjects);
              pathWriteTimes.put(parentPath, rowWriteTime);
            }

            ((ObjectNode) ref).set(p, childRef);
          }
        }
        parentRef = ref;
        ref = childRef;
        parentPath += "." + p;
      }

      if (leaf == null) {
        continue;
      }

      writeLeafIfNewer(ref, row, leaf, parentPath, pathWriteTimes, rowWriteTime, numericBooleans);
    }

    return doc;
  }

  private JsonNode changeCurrentNodeToArray(Row row, JsonNode parentRef, int pathIndex) {
    String pbefore = row.getString("p" + (pathIndex - 1));
    JsonNode ref = mapper.createArrayNode();
    if (pbefore.startsWith("[")) {
      int index = Integer.parseInt(pbefore.substring(1, pbefore.length() - 1));
      ((ArrayNode) parentRef).set(index, ref);
    } else {
      ((ObjectNode) parentRef).set(pbefore, ref);
    }

    return ref;
  }

  private JsonNode changeCurrentNodeToObject(
      Row row, JsonNode parentRef, int pathIndex, boolean writeAllPathsAsObjects) {
    String pbefore = row.getString("p" + (pathIndex - 1));
    JsonNode ref = mapper.createObjectNode();
    if (pbefore.startsWith("[") && !writeAllPathsAsObjects) {
      int index = Integer.parseInt(pbefore.substring(1, pbefore.length() - 1));
      ((ArrayNode) parentRef).set(index, ref);
    } else {
      ((ObjectNode) parentRef).set(pbefore, ref);
    }
    return ref;
  }

  private void markFullPathAsDead(
      String parentPath, String currentPath, DeadLeafCollector collector) {
    collector.addAll(parentPath + "." + currentPath);
  }

  private void markObjectAtPathAsDead(
      JsonNode ref, String parentPath, DeadLeafCollector collector) {
    if (!ref.isObject()) { // it's a scalar
      collector.addLeaf(parentPath, ImmutableDeadLeaf.builder().name("").build());
    } else {
      Iterator<String> fieldNames = ref.fieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        collector.addLeaf(parentPath, ImmutableDeadLeaf.builder().name(fieldName).build());
      }
    }
  }

  private void markArrayAtPathAsDead(JsonNode ref, String parentPath, DeadLeafCollector collector) {
    if (!ref.isArray()) { // it's a scalar
      collector.addLeaf(parentPath, ImmutableDeadLeaf.builder().name("").build());
    } else {
      collector.addArray(parentPath);
    }
  }

  private void writeLeafIfNewer(
      JsonNode ref,
      Row row,
      String leaf,
      String parentPath,
      Map<String, Long> pathWriteTimes,
      Long rowWriteTime,
      boolean numericBooleans) {
    JsonNode n = NullNode.getInstance();

    if (!row.isNull("text_value")) {
      String value = row.getString("text_value");
      if (value.equals(DocsApiConstants.EMPTY_OBJECT_MARKER)) {
        n = mapper.createObjectNode();
      } else if (value.equals(DocsApiConstants.EMPTY_ARRAY_MARKER)) {
        n = mapper.createArrayNode();
      } else {
        n = new TextNode(value);
      }
    } else if (!row.isNull("bool_value")) {
      Boolean booleanFromRow = DocsApiUtils.getBooleanFromRow(row, numericBooleans);
      n = BooleanNode.valueOf(booleanFromRow);
    } else if (!row.isNull("dbl_value")) {
      // If not a fraction represent as a long to the user
      // This lets us handle queries of doubles and longs without
      // splitting them into separate columns
      double dv = row.getDouble("dbl_value");
      long lv = (long) dv;
      if ((double) lv == dv) n = new LongNode(lv);
      else n = new DoubleNode(dv);
    }
    if (ref == null)
      throw new RuntimeException("Missing path @" + leaf + " v=" + n + " row=" + row);

    boolean shouldWrite =
        !pathWriteTimes.containsKey(parentPath + "." + leaf)
            || pathWriteTimes.get(parentPath + "." + leaf) <= rowWriteTime;
    if (shouldWrite) {
      if (ref.isObject()) {
        ((ObjectNode) ref).set(leaf, n);
      } else if (ref.isArray()) {
        if (!leaf.startsWith("["))
          throw new RuntimeException("Trying to write object to array " + leaf);

        ArrayNode arrayRef = (ArrayNode) ref;
        int index = Integer.parseInt(leaf.substring(1, leaf.length() - 1));

        int currentSize = arrayRef.size();
        for (int k = currentSize; k < index; k++) arrayRef.addNull();

        if (currentSize <= index) {
          arrayRef.add(n);
        } else if (!arrayRef.hasNonNull(index)) {
          arrayRef.set(index, n);
        }
      } else {
        throw new IllegalStateException("Invalid document state: " + ref);
      }
      pathWriteTimes.put(parentPath + "." + leaf, rowWriteTime);
    }
  }
}
