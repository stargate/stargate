package io.stargate.web.docsapi.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.service.json.DeadLeafCollector;
import io.stargate.web.docsapi.service.json.ImmutableDeadLeaf;
import java.util.*;
import javax.inject.Inject;

public class JsonConverter {
  @Inject @VisibleForTesting ObjectMapper mapper;

  // TODO find a place to refactor this to, along with DocumentService#getBooleanFromRow
  private static Boolean getBooleanFromRow(Row row, String colName, boolean numericBooleans) {
    if (row.isNull("bool_value")) return null;
    if (numericBooleans) {
      byte value = row.getByte(colName);
      return value != 0;
    }
    return row.getBoolean(colName);
  }

  public JsonNode convertToJsonDoc(
      List<Row> rows,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    JsonNode doc = mapper.createObjectNode();
    Map<String, Long> pathWriteTimes = new HashMap<>();
    if (rows.isEmpty()) {
      return doc;
    }
    Column writeTimeCol = Column.reference("writetime(leaf)");

    for (Row row : rows) {
      Long rowWriteTime = row.getLong(writeTimeCol.name());
      String rowLeaf = row.getString("leaf");
      if (rowLeaf.equals(DocumentDB.ROOT_DOC_MARKER)) {
        continue;
      }

      String leaf = null;
      JsonNode parentRef = null;
      JsonNode ref = doc;

      String parentPath = "$";

      for (int i = 0; i < DocumentDB.MAX_DEPTH; i++) {
        String p = row.getString("p" + i);
        String nextP = i < DocumentDB.MAX_DEPTH - 1 ? row.getString("p" + (i + 1)) : "";
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
          if (!ref.isArray()) {
            if (i == 0) {
              doc = mapper.createArrayNode();
              ref = doc;
            } else {
              markObjectAtPathAsDead(ref, parentPath, collector);
              ref = changeCurrentNodeToArray(row, parentRef, i);
            }
            pathWriteTimes.put(parentPath, rowWriteTime);
          }

          int index = Integer.parseInt(p.substring(1, p.length() - 1));

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
      if (value.equals(DocumentDB.EMPTY_OBJECT_MARKER)) {
        n = mapper.createObjectNode();
      } else if (value.equals(DocumentDB.EMPTY_ARRAY_MARKER)) {
        n = mapper.createArrayNode();
      } else {
        n = new TextNode(value);
      }
    } else if (!row.isNull("bool_value")) {
      n = BooleanNode.valueOf(getBooleanFromRow(row, "bool_value", numericBooleans));
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
      throw new RuntimeException("Missing path @" + leaf + " v=" + n + " row=" + row.toString());

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
