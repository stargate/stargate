/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.bridge.grpc;

import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.List;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Map;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Set;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Tuple;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

public class TypeSpecs {

  public static final TypeSpec ASCII = buildFromBasic(TypeSpec.Basic.ASCII);
  public static final TypeSpec BIGINT = buildFromBasic(TypeSpec.Basic.BIGINT);
  public static final TypeSpec BLOB = buildFromBasic(TypeSpec.Basic.BLOB);
  public static final TypeSpec BOOLEAN = buildFromBasic(TypeSpec.Basic.BOOLEAN);
  public static final TypeSpec COUNTER = buildFromBasic(TypeSpec.Basic.COUNTER);
  public static final TypeSpec DECIMAL = buildFromBasic(TypeSpec.Basic.DECIMAL);
  public static final TypeSpec DOUBLE = buildFromBasic(TypeSpec.Basic.DOUBLE);
  public static final TypeSpec FLOAT = buildFromBasic(TypeSpec.Basic.FLOAT);
  public static final TypeSpec INT = buildFromBasic(TypeSpec.Basic.INT);
  public static final TypeSpec TIMESTAMP = buildFromBasic(TypeSpec.Basic.TIMESTAMP);
  public static final TypeSpec UUID = buildFromBasic(TypeSpec.Basic.UUID);
  public static final TypeSpec VARCHAR = buildFromBasic(TypeSpec.Basic.VARCHAR);
  public static final TypeSpec VARINT = buildFromBasic(TypeSpec.Basic.VARINT);
  public static final TypeSpec TIMEUUID = buildFromBasic(TypeSpec.Basic.TIMEUUID);
  public static final TypeSpec INET = buildFromBasic(TypeSpec.Basic.INET);
  public static final TypeSpec DATE = buildFromBasic(TypeSpec.Basic.DATE);
  public static final TypeSpec TIME = buildFromBasic(TypeSpec.Basic.TIME);
  public static final TypeSpec SMALLINT = buildFromBasic(TypeSpec.Basic.SMALLINT);
  public static final TypeSpec TINYINT = buildFromBasic(TypeSpec.Basic.TINYINT);
  public static final TypeSpec DURATION = buildFromBasic(TypeSpec.Basic.DURATION);
  public static final TypeSpec LINESTRING = buildFromBasic(TypeSpec.Basic.LINESTRING);
  public static final TypeSpec POINT = buildFromBasic(TypeSpec.Basic.POINT);
  public static final TypeSpec POLYGON = buildFromBasic(TypeSpec.Basic.POLYGON);

  public static TypeSpec list(TypeSpec element) {
    return TypeSpec.newBuilder().setList(List.newBuilder().setElement(element)).build();
  }

  public static TypeSpec frozenList(TypeSpec element) {
    return TypeSpec.newBuilder()
        .setList(List.newBuilder().setElement(element).setFrozen(true))
        .build();
  }

  public static TypeSpec set(TypeSpec element) {
    return TypeSpec.newBuilder().setSet(Set.newBuilder().setElement(element)).build();
  }

  public static TypeSpec frozenSet(TypeSpec element) {
    return TypeSpec.newBuilder()
        .setSet(Set.newBuilder().setElement(element).setFrozen(true))
        .build();
  }

  public static TypeSpec map(TypeSpec key, TypeSpec value) {
    return TypeSpec.newBuilder().setMap(Map.newBuilder().setKey(key).setValue(value)).build();
  }

  public static TypeSpec frozenMap(TypeSpec key, TypeSpec value) {
    return TypeSpec.newBuilder()
        .setMap(Map.newBuilder().setKey(key).setValue(value).setFrozen(true))
        .build();
  }

  public static TypeSpec udt(String name, java.util.Map<String, TypeSpec> fields) {
    return TypeSpec.newBuilder()
        .setUdt(Udt.newBuilder().setName(name).putAllFields(fields))
        .build();
  }

  public static TypeSpec frozenUdt(String name, java.util.Map<String, TypeSpec> fields) {
    return TypeSpec.newBuilder()
        .setUdt(Udt.newBuilder().setName(name).putAllFields(fields).setFrozen(true))
        .build();
  }

  public static TypeSpec tuple(TypeSpec... elements) {
    Tuple.Builder builder = Tuple.newBuilder();
    for (TypeSpec element : elements) {
      builder.addElements(element);
    }
    return TypeSpec.newBuilder().setTuple(builder).build();
  }

  public static boolean isCollection(TypeSpec type) {
    switch (type.getSpecCase()) {
      case LIST:
      case SET:
      case MAP:
        return true;
      default:
        return false;
    }
  }

  /** Formats a type to its CQL representation (the way it would appear in a CQL query). */
  public static String format(TypeSpec type) {
    switch (type.getSpecCase()) {
      case BASIC:
        return formatBasic(type.getBasic());
      case MAP:
        Map map = type.getMap();
        return String.format(
            map.getFrozen() ? "frozen<map<%s,%s>>" : "map<%s,%s>",
            format(map.getKey()),
            format(map.getValue()));
      case LIST:
        List list = type.getList();
        return String.format(
            list.getFrozen() ? "frozen<list<%s>>" : "list<%s>", format(list.getElement()));
      case SET:
        Set set = type.getSet();
        return String.format(
            set.getFrozen() ? "frozen<set<%s>>" : "set<%s>", format(set.getElement()));
      case UDT:
        Udt udt = type.getUdt();
        return String.format(udt.getFrozen() ? "frozen<\"%s\">" : "\"%s\"", udt.getName());
      case TUPLE:
        Tuple tuple = type.getTuple();
        return tuple.getElementsList().stream()
            .map(TypeSpecs::format)
            .collect(Collectors.joining(",", "tuple<", ">"));
      default:
        throw new IllegalArgumentException("Unsupported type " + type.getSpecCase());
    }
  }

  private static String formatBasic(TypeSpec.Basic basic) {
    switch (basic) {
      case VARCHAR:
        return "text";
      case POINT:
        return "'PointType'";
      case LINESTRING:
        return "'LineStringType'";
      case POLYGON:
        return "'PolygonType'";
      default:
        return basic.name().toLowerCase();
    }
  }

  /**
   * Parses a type from its CQL string representation.
   *
   * @param udts the known UDT definitions in the keyspace, used to resolve any UDT references in
   *     the type.
   * @param strict whether to fail if non-existent UDTs are referenced. If false, missing UDTs will
   *     be resolved as a "shallow" representation (a {@link Udt} that only has its "name" field
   *     set).
   * @throws IllegalArgumentException if the type can't be parsed, or {@code strict} is true and
   *     there are unresolved UDT references.
   */
  public static TypeSpec parse(String typeString, Collection<Udt> udts, boolean strict) {
    // Parsing CQL types is a tad involved. We "brute-force" it here, but it's neither very
    // efficient, nor very elegant.

    typeString = typeString.trim();
    if (typeString.isEmpty()) {
      throw new IllegalArgumentException("Invalid empty type name");
    }

    // Special-case built-in DSE geo types, since we have gRPC basic types for them:
    switch (typeString) {
      case "'LineStringType'":
        return LINESTRING;
      case "'PointType'":
        return POINT;
      case "'PolygonType'":
        return POLYGON;
    }
    if (typeString.charAt(0) == '\'') {
      throw new IllegalArgumentException("Custom types are not supported");
    }

    int lastCharIdx = typeString.length() - 1;
    if (typeString.charAt(0) == '"') {
      // The quote should be terminated and we should have at least 1 character + the quotes,
      if (typeString.charAt(lastCharIdx) != '"' || typeString.length() < 3) {
        throw new IllegalArgumentException(
            "Malformed type name (missing closing quote): " + typeString);
      }
      String udtName = typeString.substring(1, lastCharIdx).replaceAll("\"\"", "\"");
      return findUdt(udtName, udts, strict);
    }

    int paramsIdx = typeString.indexOf('<');
    if (paramsIdx < 0) {
      return parseBasicOrUdt(typeString, udts, strict);
    } else {
      String baseTypeName = typeString.substring(0, paramsIdx).trim();
      if (typeString.charAt(lastCharIdx) != '>') {
        throw new IllegalArgumentException(
            String.format(
                "Malformed type name: parameters for type %s are missing a closing '>'",
                baseTypeName));
      }
      String paramsString = typeString.substring(paramsIdx + 1, lastCharIdx);
      java.util.List<TypeSpec> parameters =
          splitAndParseParameters(typeString, paramsString, udts, strict);
      if ("frozen".equalsIgnoreCase(baseTypeName)) {
        checkParameterCount(parameters, 1, "frozen");
        return freeze(parameters.get(0));
      } else if ("list".equalsIgnoreCase(baseTypeName)) {
        checkParameterCount(parameters, 1, "list");
        return TypeSpec.newBuilder()
            .setList(List.newBuilder().setElement(parameters.get(0)))
            .build();
      } else if ("set".equalsIgnoreCase(baseTypeName)) {
        checkParameterCount(parameters, 1, "set");
        return TypeSpec.newBuilder().setSet(Set.newBuilder().setElement(parameters.get(0))).build();
      } else if ("map".equalsIgnoreCase(baseTypeName)) {
        checkParameterCount(parameters, 2, "map");
        return TypeSpec.newBuilder()
            .setMap(Map.newBuilder().setKey(parameters.get(0)).setValue(parameters.get(1)))
            .build();
      } else if ("tuple".equalsIgnoreCase(baseTypeName)) {
        return TypeSpec.newBuilder()
            .setTuple(Tuple.newBuilder().addAllElements(parameters))
            .build();
      } else {
        throw new IllegalArgumentException(
            String.format("Malformed type name: unknown parameterized type %s", baseTypeName));
      }
    }
  }

  private static void checkParameterCount(
      java.util.List<TypeSpec> parameters, int expectedCount, String baseType) {
    if (parameters.size() != expectedCount) {
      throw new IllegalArgumentException(
          String.format(
              "Malformed type name: " + baseType + " takes only 1 parameter, but %d provided",
              parameters.size()));
    }
  }

  private static TypeSpec parseBasicOrUdt(String typeString, Collection<Udt> udts, boolean strict) {
    if ("ascii".equalsIgnoreCase(typeString)) {
      return ASCII;
    } else if ("bigint".equalsIgnoreCase(typeString)) {
      return BIGINT;
    } else if ("blob".equalsIgnoreCase(typeString)) {
      return BLOB;
    } else if ("boolean".equalsIgnoreCase(typeString)) {
      return BOOLEAN;
    } else if ("counter".equalsIgnoreCase(typeString)) {
      return COUNTER;
    } else if ("decimal".equalsIgnoreCase(typeString)) {
      return DECIMAL;
    } else if ("double".equalsIgnoreCase(typeString)) {
      return DOUBLE;
    } else if ("float".equalsIgnoreCase(typeString)) {
      return FLOAT;
    } else if ("int".equalsIgnoreCase(typeString)) {
      return INT;
    } else if ("timestamp".equalsIgnoreCase(typeString)) {
      return TIMESTAMP;
    } else if ("uuid".equalsIgnoreCase(typeString)) {
      return UUID;
    } else if ("varchar".equalsIgnoreCase(typeString) || "text".equalsIgnoreCase(typeString)) {
      return VARCHAR;
    } else if ("varint".equalsIgnoreCase(typeString)) {
      return VARINT;
    } else if ("timeuuid".equalsIgnoreCase(typeString)) {
      return TIMEUUID;
    } else if ("inet".equalsIgnoreCase(typeString)) {
      return INET;
    } else if ("date".equalsIgnoreCase(typeString)) {
      return DATE;
    } else if ("time".equalsIgnoreCase(typeString)) {
      return TIME;
    } else if ("smallint".equalsIgnoreCase(typeString)) {
      return SMALLINT;
    } else if ("tinyint".equalsIgnoreCase(typeString)) {
      return TINYINT;
    } else if ("duration".equalsIgnoreCase(typeString)) {
      return DURATION;
    } else {
      return findUdt(typeString, udts, strict);
    }
  }

  private static java.util.List<TypeSpec> splitAndParseParameters(
      String fullTypeName, String parameters, Collection<Udt> udts, boolean strict) {
    int openParam = 0;
    int currentStart = 0;
    int idx = currentStart;
    java.util.List<TypeSpec> parsedParameters = new ArrayList<>();
    while (idx < parameters.length()) {
      switch (parameters.charAt(idx)) {
        case ',':
          // Ignore if we're within a sub-parameter.
          if (openParam == 0) {
            parsedParameters.add(
                extractParameter(fullTypeName, parameters, currentStart, idx, udts, strict));
            currentStart = idx + 1;
          }
          break;
        case '<':
          ++openParam;
          break;
        case '>':
          if (--openParam < 0) {
            throw new IllegalArgumentException(
                "Malformed type name: " + fullTypeName + " (unmatched closing '>')");
          }
          break;
        case '"':
          idx = findClosingDoubleQuote(fullTypeName, parameters, idx + 1) - 1;
      }
      ++idx;
    }
    parsedParameters.add(
        extractParameter(fullTypeName, parameters, currentStart, idx, udts, strict));
    return parsedParameters;
  }

  private static TypeSpec extractParameter(
      String fullTypeName,
      String parameters,
      int start,
      int end,
      Collection<Udt> udts,
      boolean strict) {
    String parameterStr = parameters.substring(start, end);
    if (parameterStr.isEmpty()) {
      // Recursion actually handle this case, but the error thrown is a bit more cryptic in this
      // context
      throw new IllegalArgumentException("Malformed type name: " + fullTypeName);
    }
    return parse(parameterStr, udts, strict);
  }

  // Returns the index "just after the double quote", so possibly str.length.

  private static int findClosingDoubleQuote(String fullTypeName, String str, int startIdx) {
    int idx = startIdx;
    while (idx < str.length()) {
      if (str.charAt(idx) == '"') {
        // Note: 2 double-quote is a way to escape the double-quote, so move to next first and
        // only exit if it's not a double-quote. Otherwise, continue.
        ++idx;
        if (idx >= str.length() || str.charAt(idx) != '"') {
          return idx;
        }
      }
      ++idx;
    }
    throw new IllegalArgumentException("Malformed type name: " + fullTypeName);
  }

  private static TypeSpec findUdt(String udtName, Collection<Udt> udts, boolean strict) {
    Optional<Udt> maybeUdt = udts.stream().filter(u -> udtName.equals(u.getName())).findFirst();
    Udt udt =
        strict
            ? maybeUdt.orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Cannot find user type %s", udtName)))
            : maybeUdt.orElse(Udt.newBuilder().setName(udtName).build());
    return TypeSpec.newBuilder().setUdt(udt).build();
  }

  /**
   * Returns whether the type is frozen.
   *
   * <p>Note that this returns true for tuples (which are always frozen), and false for unfreezable
   * types.
   */
  public static boolean isFrozen(TypeSpec type) {
    switch (type.getSpecCase()) {
      case LIST:
        return type.getList().getFrozen();
      case SET:
        return type.getSet().getFrozen();
      case MAP:
        return type.getMap().getFrozen();
      case UDT:
        return type.getUdt().getFrozen();
      case TUPLE:
        return true;
      default:
        return false;
    }
  }

  /**
   * Returns the frozen version of the type, or the type itself if it is already frozen or is not
   * freezable.
   */
  public static TypeSpec freeze(TypeSpec type) {
    return setFrozen(type, true);
  }

  /**
   * Returns the unfrozen version of the type, or the type itself if it is already unfrozen or is
   * not unfreezable.
   */
  public static TypeSpec unfreeze(TypeSpec type) {
    return setFrozen(type, false);
  }

  private static TypeSpec setFrozen(TypeSpec type, boolean newFrozen) {
    switch (type.getSpecCase()) {
      case LIST:
        List list = type.getList();
        return list.getFrozen() == newFrozen
            ? type
            : TypeSpec.newBuilder().setList(list.toBuilder().setFrozen(newFrozen)).build();
      case SET:
        Set set = type.getSet();
        return set.getFrozen() == newFrozen
            ? type
            : TypeSpec.newBuilder().setSet(set.toBuilder().setFrozen(newFrozen)).build();
      case MAP:
        Map map = type.getMap();
        return map.getFrozen() == newFrozen
            ? type
            : TypeSpec.newBuilder().setMap(map.toBuilder().setFrozen(newFrozen)).build();
      case UDT:
        Udt udt = type.getUdt();
        return udt.getFrozen() == newFrozen
            ? type
            : TypeSpec.newBuilder().setUdt(udt.toBuilder().setFrozen(newFrozen)).build();
      default:
        return type;
    }
  }

  private static TypeSpec buildFromBasic(TypeSpec.Basic basic) {
    return TypeSpec.newBuilder().setBasic(basic).build();
  }

  private TypeSpecs() {
    // intentionally empty
  }
}
