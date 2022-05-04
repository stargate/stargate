package io.stargate.sgv2.docsapi.service.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.proto.QueryOuterClass;
import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Responsible for turning gRPC Row values into JSON. */
@Singleton
public class JsonConverter {
  private final ObjectMapper mapper;
  private final DocumentProperties docsProperties;

  @Inject
  public JsonConverter(ObjectMapper mapper, DocumentProperties docsProperties) {
    Objects.requireNonNull(mapper, "JsonConverter requires a non-null ObjectMapper");
    this.mapper = mapper;
    this.docsProperties = docsProperties;
  }

  /**
   * Converts a List of rows into a Jackson JsonNode.
   *
   * @param rows The List of rows
   * @param columns The ColumnSpec values that are in the row (in the order they appear in the Row)
   * @param writeAllPathsAsObjects `true` if array paths should be written as JSON objects instead
   * @param numericBooleans `true` if booleans should be treated as numbers
   * @return the JSON representation of the data
   */
  public JsonNode convertToJsonDoc(
      List<QueryOuterClass.Row> rows,
      List<QueryOuterClass.ColumnSpec> columns,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    return convertToJsonDoc(
        rows, columns, new NoOpDeadLeafCollector(), writeAllPathsAsObjects, numericBooleans);
  }

  /**
   * Converts a List of rows into a Jackson JsonNode.
   *
   * @param rows The List of rows
   * @param columns The ColumnSpec values that are in the row (in the order they appear in the Row)
   * @param collector a DeadLeafCollector that can be used to collect unreachable information that
   *     can be deleted.
   * @param writeAllPathsAsObjects `true` if array paths should be written as JSON objects instead
   * @param numericBooleans `true` if booleans should be treated as numbers
   * @return the JSON representation of the data
   */
  public JsonNode convertToJsonDoc(
      List<QueryOuterClass.Row> rows,
      List<QueryOuterClass.ColumnSpec> columns,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans) {
    int maxDepth = docsProperties.maxDepth();
    return convertToJsonDoc(
        rows, columns, collector, writeAllPathsAsObjects, numericBooleans, maxDepth);
  }

  /**
   * Given a list of Column Specs, generate a map of column names to indexes in the ColumnSpec list.
   *
   * @param columnSpecs The gRPC column specs
   * @return A Map of column name to the index of the column.
   */
  public Map<String, Integer> generateColumnIndexMap(List<QueryOuterClass.ColumnSpec> columnSpecs) {
    Map<String, Integer> colIndexes = new HashMap<>();
    for (int index = 0; index < columnSpecs.size(); index++) {
      QueryOuterClass.ColumnSpec col = columnSpecs.get(index);
      colIndexes.put(col.getName(), index);
    }
    return colIndexes;
  }

  /**
   * Takes a List of rows from a documents table, iterating over it and constructing a JSON object
   * (or array) that is represented by those rows. Note: Because the system avoids
   * read-before-write, it is possible for the same "path" in a document to contain two different
   * sets of data from two different versions of the document. When this is encountered during the
   * conversion, the version that has the latest writetime is accepted, and the older version is
   * collected in the DeadLeafCollector to be purged in the background.
   *
   * @param rows Rows from gRPC
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
      List<QueryOuterClass.Row> rows,
      List<QueryOuterClass.ColumnSpec> columns,
      DeadLeafCollector collector,
      boolean writeAllPathsAsObjects,
      boolean numericBooleans,
      int maxDepth) {
    JsonNode doc = mapper.createObjectNode();
    Map<String, Long> pathWriteTimes = new HashMap<>();
    if (rows.isEmpty()) {
      return doc;
    }

    Map<String, Integer> columnNameToIndex = generateColumnIndexMap(columns);

    for (QueryOuterClass.Row row : rows) {
      Long rowWriteTime = row.getValues(columnNameToIndex.get("writetime(leaf)")).getInt();
      String rowLeaf = row.getValues(columnNameToIndex.get("leaf")).getString();
      if (rowLeaf.equals(Constants.ROOT_DOC_MARKER)) {
        continue;
      }

      String leaf = null;
      JsonNode parentRef = null;
      JsonNode ref = doc;

      String parentPath = "$";

      for (int i = 0; i < maxDepth; i++) {
        String p = row.getValues(columnNameToIndex.get("p" + i)).getString();
        String nextP =
            i < maxDepth - 1 ? row.getValues(columnNameToIndex.get("p" + (i + 1))).getString() : "";
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
            ref = changeCurrentNodeToArray(row, parentRef, i, columnNameToIndex);
            pathWriteTimes.put(parentPath, rowWriteTime);
          } else if (i != 0 && !isArray && !ref.isObject()) {
            markArrayAtPathAsDead(ref, parentPath, collector);
            ref =
                changeCurrentNodeToObject(
                    row, parentRef, i, writeAllPathsAsObjects, columnNameToIndex);
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
            ref = changeCurrentNodeToArray(row, parentRef, i, columnNameToIndex);
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
              ref =
                  changeCurrentNodeToObject(
                      row, parentRef, i, writeAllPathsAsObjects, columnNameToIndex);
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

      writeLeafIfNewer(
          ref,
          row,
          leaf,
          parentPath,
          pathWriteTimes,
          rowWriteTime,
          columnNameToIndex,
          numericBooleans);
    }

    return doc;
  }

  /**
   * Given a reference to a JsonNode, turns the current data at that pointer to an ArrayNode. This
   * is necessary if an older value in the data is an Object or scalar, but a newer representation
   * is an Array.
   */
  private JsonNode changeCurrentNodeToArray(
      QueryOuterClass.Row row,
      JsonNode parentRef,
      int pathIndex,
      Map<String, Integer> columnNameToIndex) {
    String pbefore = row.getValues(columnNameToIndex.get("p" + (pathIndex - 1))).getString();
    JsonNode ref = mapper.createArrayNode();
    if (pbefore.startsWith("[")) {
      int index = Integer.parseInt(pbefore.substring(1, pbefore.length() - 1));
      ((ArrayNode) parentRef).set(index, ref);
    } else {
      ((ObjectNode) parentRef).set(pbefore, ref);
    }

    return ref;
  }

  /**
   * Given a reference to a JsonNode, turns the current data at that pointer to an ObjectNode. This
   * is necessary if an older value in the data is an Array or scalar, but a newer representation is
   * an Object.
   */
  private JsonNode changeCurrentNodeToObject(
      QueryOuterClass.Row row,
      JsonNode parentRef,
      int pathIndex,
      boolean writeAllPathsAsObjects,
      Map<String, Integer> columnNameToIndex) {
    String pbefore = row.getValues(columnNameToIndex.get("p" + (pathIndex - 1))).getString();
    JsonNode ref = mapper.createObjectNode();
    if (pbefore.startsWith("[") && !writeAllPathsAsObjects) {
      int index = Integer.parseInt(pbefore.substring(1, pbefore.length() - 1));
      ((ArrayNode) parentRef).set(index, ref);
    } else {
      ((ObjectNode) parentRef).set(pbefore, ref);
    }
    return ref;
  }

  /**
   * Adds a full path to the DeadLeafCollector.
   *
   * @param parentPath the parent path to mark for deletion.
   * @param currentPath the current path to mark for deletion.
   * @param collector the DeadLeafCollector
   */
  private void markFullPathAsDead(
      String parentPath, String currentPath, DeadLeafCollector collector) {
    collector.addAll(parentPath + "." + currentPath);
  }

  /**
   * Adds an Object's paths to the DeadLeafCollector.
   *
   * @param ref the Object to mark for deletion.
   * @param parentPath the parent path to mark for deletion.
   * @param collector the DeadLeafCollector
   */
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

  /**
   * Adds an Array's paths to the DeadLeafCollector.
   *
   * @param ref the Array to mark for deletion.
   * @param parentPath the parent path to mark for deletion.
   * @param collector the DeadLeafCollector
   */
  private void markArrayAtPathAsDead(JsonNode ref, String parentPath, DeadLeafCollector collector) {
    if (!ref.isArray()) { // it's a scalar
      collector.addLeaf(parentPath, ImmutableDeadLeaf.builder().name("").build());
    } else {
      collector.addArray(parentPath);
    }
  }

  /**
   * Writes leaf data to the JsonNode in place, but only if the writetime of the leaf is newer than
   * the current data at the path in the JsonNode.
   *
   * @param ref the JsonNode
   * @param row the Row, for extraction of values
   * @param leaf the path of the leaf
   * @param parentPath the parent path to the leaf in the document
   * @param pathWriteTimes a Map of all the path write times
   * @param rowWriteTime the current row's write time
   * @param columnNameToIndex The indexes of the columns in the row
   * @param numericBooleans whether to treat boolean values should be treated as integers
   */
  private void writeLeafIfNewer(
      JsonNode ref,
      QueryOuterClass.Row row,
      String leaf,
      String parentPath,
      Map<String, Long> pathWriteTimes,
      Long rowWriteTime,
      Map<String, Integer> columnNameToIndex,
      boolean numericBooleans) {
    JsonNode n = NullNode.getInstance();

    int txtValueIdx = columnNameToIndex.get("text_value");
    int boolValueIdx = columnNameToIndex.get("bool_value");
    int dblValueIdx = columnNameToIndex.get("dbl_value");

    if (!row.getValues(txtValueIdx).hasNull()) {
      String value = row.getValues(txtValueIdx).getString();
      if (value.equals(Constants.EMPTY_OBJECT_MARKER)) {
        n = mapper.createObjectNode();
      } else if (value.equals(Constants.EMPTY_ARRAY_MARKER)) {
        n = mapper.createArrayNode();
      } else {
        n = new TextNode(value);
      }
    } else if (!row.getValues(boolValueIdx).hasNull()) {
      QueryOuterClass.Value booleanValue = row.getValues(boolValueIdx);
      Boolean booleanFromRow;
      if (numericBooleans) {
        booleanFromRow = booleanValue.getInt() != 0;
      } else {
        booleanFromRow = booleanValue.getBoolean();
      }
      n = BooleanNode.valueOf(booleanFromRow);
    } else if (!row.getValues(dblValueIdx).hasNull()) {
      // If not a fraction represent as a long to the user
      // This lets us handle queries of doubles and longs without
      // splitting them into separate columns
      double dv = row.getValues(dblValueIdx).getDouble();
      long lv = (long) dv;
      if ((double) lv == dv) n = new LongNode(lv);
      else n = new DoubleNode(dv);
    }
    if (ref == null)
      throw new IllegalStateException("Missing path @" + leaf + " v=" + n + " row=" + row);

    boolean shouldWrite =
        !pathWriteTimes.containsKey(parentPath + "." + leaf)
            || pathWriteTimes.get(parentPath + "." + leaf) <= rowWriteTime;
    if (shouldWrite) {
      if (ref.isObject()) {
        ((ObjectNode) ref).set(leaf, n);
      } else if (ref.isArray()) {
        if (!leaf.startsWith("["))
          throw new IllegalStateException("Trying to write object to array " + leaf);

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
