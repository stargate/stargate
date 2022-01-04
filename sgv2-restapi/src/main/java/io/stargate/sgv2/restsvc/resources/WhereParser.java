package io.stargate.sgv2.restsvc.resources;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.sgv2.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.restsvc.grpc.ToProtoConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WhereParser {
  private static final ObjectMapper MAPPER =
      new ObjectMapper(
          // Throws an exception on duplicate keys like
          //    {"key1": 1, "key1": 2} or {"key1": {"eq": 1, "eq":2}}
          JsonFactory.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build());

  private final Schema.CqlTable tableDef;
  private final ToProtoConverter converter;

  public WhereParser(Schema.CqlTable tableDef, ToProtoConverter converter) {
    this.tableDef = tableDef;
    this.converter = converter;
  }

  public List<BuiltCondition> parseWhere(
      String whereParam, QueryOuterClass.Values.Builder valuesBuilder) {
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

    for (Map.Entry<String, JsonNode> whereCondition : asIterable(jsonTree.fields())) {
      String fieldName = whereCondition.getKey();
      JsonNode condition = whereCondition.getValue();
      if (!condition.isObject()) {
        throw new IllegalArgumentException(
            String.format("Entry for field %s was expecting a JSON object as input.", fieldName));
      }

      for (Map.Entry<String, JsonNode> operandAndValue : asIterable(condition.fields())) {
        String rawOp = operandAndValue.getKey();
        FilterOp operator = FilterOp.decode(rawOp);

        JsonNode valueNode = operandAndValue.getValue();
        if (valueNode.isNull()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value entry for field %s, operation %s was expecting a value, but found null.",
                  fieldName, rawOp));
        }

        switch (operator) {
          case $IN:
            addInOperation(conditions, fieldName, valueNode, valuesBuilder);
            break;
            //            evaluateIn(conditions, context);
          case $EXISTS:
            //            evaluateExists(conditions, context);
          case $CONTAINS:
            //            evaluateContains(conditions, context);
          case $CONTAINSKEY:
            //            evaluateContainsKey(conditions, context);
          case $CONTAINSENTRY:
            //            evaluateContainsEntry(conditions, context);
            throw new IllegalArgumentException("Operation " + operator + " not yet supported");
          default: // GT, GTE, LT, LTE, EQ, NE
            addSimpleOperation(conditions, fieldName, operator, valueNode, valuesBuilder);
        }
      }
    }

    return conditions;
  }

  private static <T> Iterable<T> asIterable(Iterator<T> iterator) {
    return () -> iterator;
  }

  private void addSimpleOperation(
      List<BuiltCondition> conditions,
      String fieldName,
      FilterOp filterOp,
      JsonNode valueNode,
      QueryOuterClass.Values.Builder valuesBuilder) {
    if (valueNode.isTextual()) {
      addSingleSimpleCondition(
          conditions, fieldName, filterOp, valueNode.textValue(), valuesBuilder);
    } else if (valueNode.isArray()) {
      for (JsonNode element : valueNode) {
        addSingleSimpleCondition(
            conditions, fieldName, filterOp, nodeToRawObject(element), valuesBuilder);
      }
    } else {
      addSingleSimpleCondition(
          conditions, fieldName, filterOp, nodeToRawObject(valueNode), valuesBuilder);
    }
  }

  private void addSingleSimpleCondition(
      List<BuiltCondition> conditions,
      String fieldName,
      FilterOp filterOp,
      Object rawValue,
      QueryOuterClass.Values.Builder valuesBuilder) {
    final QueryOuterClass.Value opValue;
    if (rawValue instanceof String) {
      opValue = converter.protoValueFromStringified(fieldName, (String) rawValue);
    } else {
      opValue = converter.protoValueFromStrictlyTyped(fieldName, rawValue);
    }
    conditions.add(BuiltCondition.ofMarker(fieldName, filterOp.predicate));
    valuesBuilder.addValues(opValue);
  }

  private void addInOperation(
      List<BuiltCondition> conditions,
      String fieldName,
      JsonNode valueNode,
      QueryOuterClass.Values.Builder valuesBuilder) {
    if (!valueNode.isArray() || valueNode.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field '%s', operation %s must be a non-empty array",
              fieldName, FilterOp.$IN.rawValue));
    }
    List<QueryOuterClass.Value> inValues = new ArrayList<>();
    for (JsonNode element : valueNode) {
      final Object rawElement = nodeToRawObject(element);
      inValues.add(converter.protoValueFromStrictlyTyped(fieldName, rawElement));
    }
    conditions.add(BuiltCondition.ofMarker(fieldName, Predicate.IN));
    valuesBuilder.addValues(Values.of(inValues));
  }

  private Object nodeToRawObject(JsonNode valueNode) {
    try {
      return MAPPER.treeToValue(valueNode, Object.class);
    } catch (IOException e) {
      // Should be impossible as all JsonNodes map to a java.lang.Object, but is declared so:
      throw new IllegalArgumentException(e);
    }
  }

  /*
  private static void evaluateContainsKey(List<BuiltCondition> conditions, QueryContext context) {
    if (context.type == null || !context.type.isMap()) {
      throw new IllegalArgumentException(
          String.format(
              "Field %s: operation %s is only supported for map types",
              context.fieldName, context.rawOp));
    }
    context.type = context.type.parameters().get(0);
    addToCondition(conditions, context);
  }
    */

  /*
  private static void evaluateContains(List<BuiltCondition> conditions, QueryContext context) {
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
  }
     */

  /*
  private static void evaluateExists(List<BuiltCondition> conditions, QueryContext context) {
    if (!context.value.isBoolean() || !context.value.booleanValue()) {
      throw new IllegalArgumentException("`exists` only supports the value `true`.");
    }
    conditions.add(conditionToWhere(context.fieldName, context.operator, true));
  }
     */
  /*

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
  }
     */

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
}
