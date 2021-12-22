package io.stargate.sgv2.restsvc.resources;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.sgv2.restsvc.grpc.BridgeProtoTypeTranslator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WhereParser {
  private static final ObjectMapper MAPPER =
      new ObjectMapper(
          // Throws an exception on duplicate keys like
          //    {"key1": 1, "key1": 2} or {"key1": {"eq": 1, "eq":2}}
          JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

  static class TableForWhere {
    private final Map<String, String> columnTypes;

    private TableForWhere(Map<String, String> columnTypes) {
      this.columnTypes = columnTypes;
    }

    public static TableForWhere create(Schema.CqlTable tableDef) {
      Map<String, String> columnMap = new HashMap<>();
      addColumns(columnMap, tableDef.getPartitionKeyColumnsList());
      addColumns(columnMap, tableDef.getClusteringKeyColumnsList());
      addColumns(columnMap, tableDef.getStaticColumnsList());
      addColumns(columnMap, tableDef.getColumnsList());
      return new TableForWhere(columnMap);
    }

    private static void addColumns(
        Map<String, String> columnTypes, List<QueryOuterClass.ColumnSpec> columnDefs) {
      for (QueryOuterClass.ColumnSpec columnDef : columnDefs) {
        // false -> do NOT include "frozen<...>" around definition
        final String type =
            BridgeProtoTypeTranslator.cqlTypeFromBridgeTypeSpec(columnDef.getType(), false);
        // To support case-insensitive lookups, add both original and (if different), lower-cased
        String name = columnDef.getName();
        columnTypes.put(name, type);
        String lcName = name.toLowerCase();
        if (lcName != name) { // toLowerCase() returns original String if no changes
          columnTypes.put(lcName, type);
        }
      }
    }

    public String findType(String columnName) {
      // Assume in most cases
      String type = columnTypes.get(columnName);
      // Need to allow case-insensitive lookups as well
      if (type == null) {
        type = columnTypes.get(columnName.toLowerCase());
      }
      return type;
    }
  }

  public static List<BuiltCondition> parseWhere(String whereParam, Schema.CqlTable rawTableDef)
      throws IOException {
    JsonNode jsonTree;
    try {
      jsonTree = MAPPER.readTree(whereParam);
      if (!jsonTree.isObject()) {
        throw new IllegalArgumentException(
            "Was expecting a JSON object as input for where parameter.");
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException(
          String.format("Input provided is not valid json. %s", e.getMessage()));
    }

    final List<BuiltCondition> conditions = new ArrayList<>();
    TableForWhere tableDef = TableForWhere.create(rawTableDef);

    for (Map.Entry<String, JsonNode> whereCondition : asIterable(jsonTree.fields())) {
      String fieldName = whereCondition.getKey();
      String fieldType = tableDef.findType(fieldName);
      if (fieldType == null) {
        throw new IllegalArgumentException(String.format("Unknown field name '%s'.", fieldName));
      }
      JsonNode condition = whereCondition.getValue();
      if (!condition.isObject()) {
        throw new IllegalArgumentException(
            String.format("Entry for field %s was expecting a JSON object as input.", fieldName));
      }

      for (Map.Entry<String, JsonNode> operandAndValue : asIterable(condition.fields())) {
        String rawOp = operandAndValue.getKey();
        FilterOp operator = FilterOp.decode(rawOp);

        JsonNode value = operandAndValue.getValue();
        if (value.isNull()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value entry for field %s, operation %s was expecting a value, but found null.",
                  fieldName, rawOp));
        }

        QueryContext context = new QueryContext();
        context.fieldName = fieldName;
        context.rawOp = rawOp;
        context.operator = operator;
        context.value = value;
        context.type = fieldType;

        switch (operator) {
          case $IN:
            evaluateIn(conditions, context);
            break;
          case $EXISTS:
            evaluateExists(conditions, context);
            break;
          case $CONTAINS:
            evaluateContains(conditions, context);
            break;
          case $CONTAINSKEY:
            evaluateContainsKey(conditions, context);
            break;
          case $CONTAINSENTRY:
            evaluateContainsEntry(conditions, context);
            break;
          default: // GT, GTE, LT, LTE, EQ, NE
            {
              addToCondition(conditions, context);
              break;
            }
        }
      }
    }

    return conditions;
  }

  private static <T> Iterable<T> asIterable(Iterator<T> iterator) {
    return () -> iterator;
  }

  private static void addToCondition(List<BuiltCondition> conditions, QueryContext context) {
    /*
    if (context.value.isArray()) {
      for (JsonNode element : asIterable(context.value.elements())) {
        Object val = Converters.toCqlValue(context.type, element.asText());
        conditions.add(conditionToWhere(context.fieldName, context.operator, val));
      }
    } else {
      Object val = Converters.toCqlValue(context.type, context.value.asText());
      conditions.add(conditionToWhere(context.fieldName, context.operator, val));
    }
     */
  }

  /*
  private static BuiltCondition conditionToWhere(String fieldName, FilterOp op, Object value) {
    return BuiltCondition.of(fieldName, op.predicate, value);
  }
   */

  private static void evaluateContainsKey(List<BuiltCondition> conditions, QueryContext context) {
    /*
    if (context.type == null || !context.type.isMap()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for map types",
              context.fieldName, context.rawOp));
    }
    context.type = context.type.parameters().get(0);
    addToCondition(conditions, context);
    */
  }

  private static void evaluateContains(List<BuiltCondition> conditions, QueryContext context) {
    /*
    }
    if (context.type == null || !context.type.isCollection()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for collection types",
              context.fieldName, context.rawOp));
    }
    context.type =
        context.type.isMap() ? context.type.parameters().get(1) : context.type.parameters().get(0);
    addToCondition(conditions, context);
     */
  }

  private static void evaluateExists(List<BuiltCondition> conditions, QueryContext context) {
    /*
    if (!context.value.isBoolean() || !context.value.booleanValue()) {
      throw new IllegalArgumentException("`exists` only supports the value `true`.");
    }
    conditions.add(conditionToWhere(context.fieldName, context.operator, true));
     */
  }

  private static void evaluateContainsEntry(List<BuiltCondition> conditions, QueryContext context) {
    if (context.value.isObject()) {
      addEntryCondition(conditions, context);
    } else if (context.value.isArray()) {
      JsonNode entries = context.value;
      for (JsonNode entry : asIterable(entries.elements())) {
        context.value = entry;
        addEntryCondition(conditions, context);
      }
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an object "
                  + "with two fields 'key' and 'value' or an array of those objects.",
              context.fieldName, context.rawOp));
    }
  }

  private static void addEntryCondition(List<BuiltCondition> conditions, QueryContext context) {
    JsonNode entryKey, entryValue;
    if (!context.value.isObject()
        || context.value.size() != 2
        || (entryKey = context.value.get("key")) == null
        || (entryValue = context.value.get("value")) == null) {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an object "
                  + "with two fields 'key' and 'value'.",
              context.fieldName, context.rawOp));
    }

    /*
    if (context.type == null || !context.type.isMap()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for map types",
              context.fieldName, context.rawOp));
    }
    Column.ColumnType keyType = context.type.parameters().get(0);
    Column.ColumnType valueType = context.type.parameters().get(1);
    Object mapKey = Converters.toCqlValue(keyType, entryKey.asText());
    Object mapValue = Converters.toCqlValue(valueType, entryValue.asText());
    conditions.add(
        BuiltCondition.of(
            LHS.mapAccess(context.fieldName, mapKey), context.operator.predicate, mapValue));
     */
  }

  private static void evaluateIn(List<BuiltCondition> conditions, QueryContext context)
      throws IOException {
    if (!context.value.isArray()) {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an array.",
              context.fieldName, context.rawOp));
    }
    ObjectReader reader = MAPPER.readerFor(new TypeReference<List<Object>>() {});

    List<Object> rawList = reader.readValue(context.value);
    /*
    conditions.add(
        conditionToWhere(
            context.fieldName,
            context.operator,
            rawList.stream()
                .map(obj -> Converters.toCqlValue(context.type, obj))
                .collect(Collectors.toList())));
     */
  }

  public static class BuiltCondition {
    public final String columnName;
    public final Predicate predicate;
    public final Object value;

    public BuiltCondition(String columnName, Predicate predicate, Object value) {
      this.columnName = columnName;
      this.predicate = predicate;
      this.value = value;
    }
  }

  private static class QueryContext {
    public String fieldName;
    public String rawOp;
    public FilterOp operator;
    public JsonNode value;
    public String type; // full CQL type definition NOT including "frozen" marker
  }

  enum FilterOp {
    $EQ("==", Predicate.EQ, "$eq"),
    $LT("<", Predicate.LT, "$lt"),
    $LTE("<=", Predicate.LTE, "$lte"),
    $GT(">", Predicate.GT, "$gt"),
    $GTE(">=", Predicate.GTE, "$gte"),
    $EXISTS("==", Predicate.EQ, "$exists"),
    $IN("IN", Predicate.IN, "$in"),
    $CONTAINS("CONTAINS", Predicate.CONTAINS, "$contains"),
    $CONTAINSKEY("CONTAINS KEY", Predicate.CONTAINS_KEY, "$containsKey"),
    $CONTAINSENTRY("==", Predicate.EQ, "$containsEntry"),
    ;
    // NE("!=", WhereCondition.Predicate.Neq) CQL 3.4.5 doesn't support <>
    // NIN(...) CQL 3.4.5 doesn't support NOT IN

    public final String cqlOp;
    public final Predicate predicate;
    public final String rawValue;

    FilterOp(String name, Predicate predicate, String rawValue) {
      this.cqlOp = name;
      this.predicate = predicate;
      this.rawValue = rawValue;
    }

    public static FilterOp decode(String rawOp) {
      try {
        return FilterOp.valueOf(rawOp.toUpperCase());
      } catch (IllegalArgumentException iea) {
        throw new IllegalArgumentException(String.format("Operation %s is not supported.", rawOp));
      }
    }
  }

  enum Predicate {
    EQ("="),
    NEQ("!="),
    LT("<"),
    GT(">"),
    LTE("<="),
    GTE(">="),
    IN("IN"),
    CONTAINS("CONTAINS"),
    CONTAINS_KEY("CONTAINS KEY");

    private final String cql;

    Predicate(String cql) {
      this.cql = cql;
    }

    @Override
    public String toString() {
      return cql;
    }
  }
}
