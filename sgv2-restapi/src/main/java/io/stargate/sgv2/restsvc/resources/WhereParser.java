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
import io.stargate.sgv2.common.cql.builder.Value;
import io.stargate.sgv2.restsvc.grpc.ToProtoConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Helper class that handles parsing of "WHERE" clause for GET rows endpoint.
 *
 * <p>Note: code modified from StargateV1 "WhereParser" ({@code
 * io.stargate.web.service.WhereParser}) which unfortunately could not be used as-is due to
 * dependencies on SGv1 to persistence type system.
 */
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
        FilterOp operation = FilterOp.decode(rawOp);

        JsonNode valueNode = operandAndValue.getValue();
        if (valueNode.isNull()) {
          throw new IllegalArgumentException(
              String.format(
                  "Value entry for field %s, operation %s was expecting a value, but found null.",
                  fieldName, rawOp));
        }

        switch (operation) {
          case $IN:
            addInOperation(conditions, fieldName, valueNode, valuesBuilder);
            break;
          case $EXISTS:
            addExistsOperation(conditions, fieldName, valueNode, valuesBuilder);
            break;
          case $CONTAINS:
            addContainsOperation(conditions, fieldName, valueNode, valuesBuilder);
            break;
          case $CONTAINSKEY:
            addContainsKeyOperation(conditions, fieldName, valueNode, valuesBuilder);
            break;
          case $CONTAINSENTRY:
            addContainsEntryOperation(conditions, fieldName, valueNode, valuesBuilder);
            break;
          default: // GT, GTE, LT, LTE, EQ, NE
            addSimpleOperation(conditions, fieldName, operation, valueNode, valuesBuilder);
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
      addSingleSimpleOperation(
          conditions, fieldName, filterOp, valueNode.textValue(), valuesBuilder);
    } else if (valueNode.isArray()) {
      for (JsonNode element : valueNode) {
        addSingleSimpleOperation(
            conditions, fieldName, filterOp, nodeToRawObject(element), valuesBuilder);
      }
    } else {
      addSingleSimpleOperation(
          conditions, fieldName, filterOp, nodeToRawObject(valueNode), valuesBuilder);
    }
  }

  private void addSingleSimpleOperation(
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

  private void addContainsOperation(
      List<BuiltCondition> conditions,
      String fieldName,
      JsonNode valueNode,
      QueryOuterClass.Values.Builder valuesBuilder) {
    // First convert from JsonNode into "natural" Java Object (scalar, List, Map etc)
    final Object rawValue = nodeToRawObject(valueNode);
    // And then try to decode into "Content" value of Container type
    final QueryOuterClass.Value opValue =
        converter.contentProtoValueFromLooselyTyped(fieldName, rawValue);
    // For containers results is always non-null (Bridge/gRPC has marker for "null" values");
    // non-container types return null as marker
    if (opValue == null) {
      throw new IllegalArgumentException(
          String.format(
              "Field '%s' not of container type (list, map, set); has to be for operation %s",
              fieldName, FilterOp.$CONTAINS.rawValue));
    }
    conditions.add(BuiltCondition.ofMarker(fieldName, FilterOp.$CONTAINS.predicate));
    valuesBuilder.addValues(opValue);
  }

  private void addContainsKeyOperation(
      List<BuiltCondition> conditions,
      String fieldName,
      JsonNode valueNode,
      QueryOuterClass.Values.Builder valuesBuilder) {
    // First convert from JsonNode into "natural" Java Object (scalar, List, Map etc)
    final Object rawValue = nodeToRawObject(valueNode);
    // And then try to decode into "Key" value of Map type
    final QueryOuterClass.Value opValue =
        converter.keyProtoValueFromLooselyTyped(fieldName, rawValue);
    // For containers results is always non-null (Bridge/gRPC has marker for "null" values");
    // non-container types return null as marker
    if (opValue == null) {
      throw new IllegalArgumentException(
          String.format(
              "Field '%s' not of map type; has to be for operation %s",
              fieldName, FilterOp.$CONTAINSKEY.rawValue));
    }
    conditions.add(BuiltCondition.ofMarker(fieldName, FilterOp.$CONTAINSKEY.predicate));
    valuesBuilder.addValues(opValue);
  }

  private void addContainsEntryOperation(
      List<BuiltCondition> conditions,
      String fieldName,
      JsonNode entryNode,
      QueryOuterClass.Values.Builder valuesBuilder) {
    // First: we need a JSON Object with "key" and "value"
    JsonNode keyNode = entryNode.get("key");
    JsonNode valueNode = entryNode.get("value");
    if ((keyNode == null) || (valueNode == null)) {
      throw new IllegalArgumentException(
          String.format(
              "Value entry for field %s, operation %s must be an object with two fields 'key' and 'value'",
              fieldName, FilterOp.$CONTAINSENTRY.rawValue));
    }

    // First convert from JsonNode into "natural" Java Object (scalar, List, Map etc)
    final Object rawKey = nodeToRawObject(keyNode);
    final Object rawValue = nodeToRawObject(valueNode);

    // Looks like we cannot pass Key as bound parameter, so no need to convert.
    // But we do want to check field is of Map type anyway so
    // And then try to decode Key and Value to match
    if (converter.getCodec(fieldName).getKeyCodec() == null) {
      throw new IllegalArgumentException(
          String.format(
              "Field '%s' not of map type; has to be for operation %s",
              fieldName, FilterOp.$CONTAINSENTRY.rawValue));
    }
    // But we can pass value via placeholder
    final QueryOuterClass.Value opContentValue =
        converter.contentProtoValueFromLooselyTyped(fieldName, rawValue);

    conditions.add(
        BuiltCondition.of(
            BuiltCondition.LHS.mapAccess(fieldName, rawKey),
            FilterOp.$CONTAINSENTRY.predicate,
            Value.marker()));
    valuesBuilder.addValues(opContentValue);
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

  private void addExistsOperation(
      List<BuiltCondition> conditions,
      String fieldName,
      JsonNode valueNode,
      QueryOuterClass.Values.Builder valuesBuilder) {
    if (!valueNode.isBoolean() || !valueNode.booleanValue()) {
      throw new IllegalArgumentException(
          String.format("`%s` only supports the value `true`.", FilterOp.$EXISTS.rawValue));
    }
    // Could also fetch field metadata to verify it is Boolean. But persistence can verify that

    // 05-Jan-2022, tatu: As per [https://github.com/stargate/stargate/discussions/1519]
    //   behavior with REST API (vs Documents API) is problematic. We implement it the same way
    //   for StargateV2 as for V1, for compatibility reasons.
    addSingleSimpleOperation(conditions, fieldName, FilterOp.$EXISTS, Boolean.TRUE, valuesBuilder);
  }

  private Object nodeToRawObject(JsonNode valueNode) {
    try {
      return MAPPER.treeToValue(valueNode, Object.class);
    } catch (IOException e) {
      // Should be impossible as all JsonNodes map to a java.lang.Object, but is declared so:
      throw new IllegalArgumentException(e);
    }
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
}
