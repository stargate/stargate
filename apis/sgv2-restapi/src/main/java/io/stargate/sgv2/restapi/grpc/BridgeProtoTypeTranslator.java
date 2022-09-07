package io.stargate.sgv2.restapi.grpc;

import io.stargate.bridge.proto.QueryOuterClass;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class that contains functionality for translating between type names used by REST/CQL and
 * Bridge/gRPC when describing things like column types.
 */
public class BridgeProtoTypeTranslator {
  /**
   * Method for constructing CQL type definition String like {@code map<string, uuid>} from
   * Bridge/gRPC {@code TypeSpec}: type definition String should be something that may be included
   * directly in a CQL statement
   *
   * @param type Bridge/gRPC type specification of type
   * @param includeFrozen Whether to include surrounding "frozen<...></...> in result or not
   * @return Valid CQL type definition String that matches given type specification
   */
  public static String cqlTypeFromBridgeTypeSpec(
      QueryOuterClass.TypeSpec type, boolean includeFrozen) {
    boolean frozen;
    String desc;

    switch (type.getSpecCase()) {
      case BASIC:
        return basicBridgeToCqlType(type.getBasic());
      case LIST:
        {
          QueryOuterClass.TypeSpec.List listType = type.getList();
          desc = "list<" + cqlTypeFromBridgeTypeSpec(listType.getElement(), includeFrozen) + ">";
          frozen = listType.getFrozen();
          break;
        }
      case MAP:
        {
          QueryOuterClass.TypeSpec.Map mapType = type.getMap();
          desc =
              "map<"
                  + cqlTypeFromBridgeTypeSpec(mapType.getKey(), includeFrozen)
                  + ", "
                  + cqlTypeFromBridgeTypeSpec(mapType.getValue(), includeFrozen)
                  + ">";
          frozen = mapType.getFrozen();
          break;
        }
      case SET:
        {
          QueryOuterClass.TypeSpec.Set setType = type.getSet();
          desc = "set<" + cqlTypeFromBridgeTypeSpec(setType.getElement(), includeFrozen) + ">";
          frozen = setType.getFrozen();
          break;
        }
      case TUPLE:
        {
          QueryOuterClass.TypeSpec.Tuple tupleType = type.getTuple();
          String types =
              tupleType.getElementsList().stream()
                  .map(elem -> cqlTypeFromBridgeTypeSpec(elem, includeFrozen))
                  .collect(Collectors.joining(", "));
          desc = "tuple<" + types + ">";
          frozen = false;
          break;
        }
      case UDT:
        {
          QueryOuterClass.TypeSpec.Udt udtType = type.getUdt();
          desc = "udt<" + udtType.getName() + ">";
          frozen = udtType.getFrozen();
          break;
        }
      case SPEC_NOT_SET:
      default:
        throw new IllegalArgumentException("Undefined/unrecognized TypeSpec: " + type);
    }
    if (includeFrozen && frozen) {
      desc = "frozen<" + desc + ">";
    }
    return desc;
  }

  private static String basicBridgeToCqlType(QueryOuterClass.TypeSpec.Basic type) {
    // For now we should have full mapping available:
    String cqlBasicType = BridgeToCqlTypesHelper.findBasicCqlType(type);
    if (cqlBasicType == null) {
      throw new IllegalArgumentException("Unrecognized TypeSpec.Basic: " + type.name());
    }
    return cqlBasicType;
  }

  /**
   * Method for constructing valid Bridge/gRPC {@code TypeSpec} given a valid CQL type description
   * String.
   *
   * @param cqlType Valid CQL type description String (like {code String} or {@code map<string,
   *     uuid>}.
   * @return Valid {@code TypeSpec} that defines same type as the given CQL type description.
   */
  public static QueryOuterClass.TypeSpec cqlTypeFromBridgeTypeSpec(String cqlType) {
    cqlType = cqlType.trim();
    if (cqlType.isEmpty()) {
      throw new IllegalArgumentException("Invalid empty cql type name");
    }
    return CqlTypeParser.parse(cqlType, false);
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Helper classes
  /////////////////////////////////////////////////////////////////////////
   */

  private static class CqlToBridgeTypesHelper {
    private final Map<String, QueryOuterClass.TypeSpec> simpleCqlToBridgeTypes;

    private static final CqlToBridgeTypesHelper instance = new CqlToBridgeTypesHelper();

    CqlToBridgeTypesHelper() {
      simpleCqlToBridgeTypes = new LinkedHashMap<>();
      for (QueryOuterClass.TypeSpec.Basic basicType : QueryOuterClass.TypeSpec.Basic.values()) {
        // 29-Nov-2021, tatu: Lovely! Trying to get name of this extra entry will explode
        //    everything... so must avoid, explicitly.
        if (basicType == QueryOuterClass.TypeSpec.Basic.UNRECOGNIZED) {
          continue;
        }
        // At least for now, there is full 1-to-1 mapping between enum upper-case name, matching
        // lower-case CQL type name
        final String cqlTypeName = basicType.name().toLowerCase();
        QueryOuterClass.TypeSpec bridgeType =
            QueryOuterClass.TypeSpec.newBuilder().setBasic(basicType).build();
        simpleCqlToBridgeTypes.put(cqlTypeName, bridgeType);
      }
    }

    public static QueryOuterClass.TypeSpec findBasicBridgeTypeCaseInsensitive(String cqlType) {
      return instance.find(cqlType);
    }

    private QueryOuterClass.TypeSpec find(String cqlType) {
      QueryOuterClass.TypeSpec bridgeType = simpleCqlToBridgeTypes.get(cqlType);
      if (bridgeType == null) {
        String lc = cqlType.toLowerCase(Locale.ROOT);
        if (lc != cqlType) {
          bridgeType = simpleCqlToBridgeTypes.get(lc);
        }
      }
      return bridgeType;
    }
  }

  private static class BridgeToCqlTypesHelper {
    private static final EnumMap<QueryOuterClass.TypeSpec.Basic, String> simpleBridgeToCqlTypes;

    static {
      simpleBridgeToCqlTypes = new EnumMap<>(QueryOuterClass.TypeSpec.Basic.class);
      for (QueryOuterClass.TypeSpec.Basic basicType : QueryOuterClass.TypeSpec.Basic.values()) {
        String cqlName = basicType.name().toLowerCase();
        // 06-Jan-2021, tatu: One exception; let's map "Varchar" to "Text". Not the cleanest
        //    way but has to do since internally Varchar is still used:
        if ("varchar".equals(cqlName)) {
          cqlName = "text";
        }
        simpleBridgeToCqlTypes.put(basicType, cqlName);
      }
    }

    public static String findBasicCqlType(QueryOuterClass.TypeSpec.Basic type) {
      return simpleBridgeToCqlTypes.get(type);
    }
  }

  // Lifted from "DataTypeCqlNameParser" of DataStax OSS Java driver
  private static class CqlTypeParser {
    public static QueryOuterClass.TypeSpec parse(String toParse, boolean frozen) {
      if (toParse.startsWith("'")) {
        throw new IllegalArgumentException("Custom types are not supported");
      }
      CqlTypeScanner scanner = new CqlTypeScanner(toParse, 0);
      String type = scanner.parseTypeName();

      QueryOuterClass.TypeSpec nativeType =
          new CqlToBridgeTypesHelper().findBasicBridgeTypeCaseInsensitive(type);
      if (nativeType != null) {
        return nativeType;
      }

      if (scanner.isEOS()) {
        // No parameters => it's a UDT
        return QueryOuterClass.TypeSpec.newBuilder().setUdt(udtType(type, frozen)).build();
      }

      List<String> parameters = scanner.parseTypeParameters();
      if (type.equalsIgnoreCase("list")) {
        if (parameters.size() != 1) {
          throw new IllegalArgumentException(
              String.format("Expecting single parameter for list, got %s", parameters));
        }
        QueryOuterClass.TypeSpec elementType = parse(parameters.get(0), false);
        return QueryOuterClass.TypeSpec.newBuilder().setList(listType(elementType, frozen)).build();
      }

      if (type.equalsIgnoreCase("set")) {
        if (parameters.size() != 1) {
          throw new IllegalArgumentException(
              String.format("Expecting single parameter for set, got %s", parameters));
        }
        QueryOuterClass.TypeSpec elementType = parse(parameters.get(0), false);
        return QueryOuterClass.TypeSpec.newBuilder().setSet(setType(elementType, frozen)).build();
      }

      if (type.equalsIgnoreCase("map")) {
        if (parameters.size() != 2) {
          throw new IllegalArgumentException(
              String.format("Expecting two parameters for map, got %s", parameters));
        }
        QueryOuterClass.TypeSpec keyType = parse(parameters.get(0), false);
        QueryOuterClass.TypeSpec valueType = parse(parameters.get(1), false);
        return QueryOuterClass.TypeSpec.newBuilder()
            .setMap(mapType(keyType, valueType, frozen))
            .build();
      }

      if (type.equalsIgnoreCase("frozen")) {
        if (parameters.size() != 1) {
          throw new IllegalArgumentException(
              String.format(
                  "Expecting single parameter for frozen keyword, got %d: %s",
                  parameters.size(), parameters));
        }
        return parse(parameters.get(0), true);
      }

      if (type.equalsIgnoreCase("tuple")) {
        if (parameters.isEmpty()) {
          throw new IllegalArgumentException(
              "Expecting at least one parameter for tuple, got none");
        }
        List<QueryOuterClass.TypeSpec> componentTypes = new ArrayList<>();
        for (String rawType : parameters) {
          componentTypes.add(parse(rawType, false));
        }
        return QueryOuterClass.TypeSpec.newBuilder().setTuple(tupleType(componentTypes)).build();
      }

      throw new IllegalArgumentException("Could not parse type name '" + toParse + "'");
    }

    private static QueryOuterClass.TypeSpec.List listType(
        QueryOuterClass.TypeSpec element, boolean frozen) {
      return QueryOuterClass.TypeSpec.List.newBuilder()
          .setFrozen(frozen)
          .setElement(element)
          .build();
    }

    private static QueryOuterClass.TypeSpec.Map mapType(
        QueryOuterClass.TypeSpec keyType, QueryOuterClass.TypeSpec valueType, boolean frozen) {
      return QueryOuterClass.TypeSpec.Map.newBuilder()
          .setFrozen(frozen)
          .setKey(keyType)
          .setValue(valueType)
          .build();
    }

    private static QueryOuterClass.TypeSpec.Set setType(
        QueryOuterClass.TypeSpec element, boolean frozen) {
      return QueryOuterClass.TypeSpec.Set.newBuilder()
          .setFrozen(frozen)
          .setElement(element)
          .build();
    }

    private static QueryOuterClass.TypeSpec.Tuple tupleType(
        List<QueryOuterClass.TypeSpec> elementTypes) {
      QueryOuterClass.TypeSpec.Tuple.Builder b = QueryOuterClass.TypeSpec.Tuple.newBuilder();
      for (QueryOuterClass.TypeSpec type : elementTypes) {
        b.addElements(type);
      }
      return b.build();
    }

    private static QueryOuterClass.TypeSpec.Udt udtType(String name, boolean frozen) {
      return QueryOuterClass.TypeSpec.Udt.newBuilder().setName(name).setFrozen(frozen).build();
    }
  }

  private static class CqlTypeScanner {
    private final String str;
    private int idx;

    CqlTypeScanner(String str, int idx) {
      this.str = str;
      this.idx = idx;
    }

    String parseTypeName() {
      idx = skipSpaces(str, idx);
      return readNextIdentifier();
    }

    List<String> parseTypeParameters() {
      List<String> list = new ArrayList<>();

      if (isEOS()) {
        return list;
      }

      skipBlankAndComma();

      if (str.charAt(idx) != '<') {
        throw new IllegalStateException();
      }

      ++idx; // skipping '<'

      while (skipBlankAndComma()) {
        if (str.charAt(idx) == '>') {
          ++idx;
          return list;
        }

        String name = parseTypeName();
        String args = readRawTypeParameters();
        list.add(name + args);
      }
      throw new IllegalArgumentException(
          String.format(
              "Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    // left idx positioned on the character stopping the read
    private String readNextIdentifier() {
      int startIdx = idx;
      if (str.charAt(startIdx) == '"') { // case-sensitive name included in double quotes
        ++idx;
        // read until closing quote.
        while (!isEOS()) {
          boolean atQuote = str.charAt(idx) == '"';
          ++idx;
          if (atQuote) {
            // if the next character is also a quote, this is an escaped quote, continue reading,
            // otherwise stop.
            if (!isEOS() && str.charAt(idx) == '"') {
              ++idx;
            } else {
              break;
            }
          }
        }
      } else if (str.charAt(startIdx) == '\'') { // custom type name included in single quotes
        ++idx;
        // read until closing quote.
        while (!isEOS() && str.charAt(idx++) != '\'') {
          /* loop */
        }
      } else {
        while (!isEOS() && (isCqlIdentifierChar(str.charAt(idx)) || str.charAt(idx) == '"')) {
          ++idx;
        }
      }
      return str.substring(startIdx, idx);
    }

    // Assumes we have just read a type name and read its potential arguments blindly. I.e. it
    // assumes that either parsing is done or that we're on a '<' and this reads everything up until
    // the corresponding closing '>'. It returns everything read, including the enclosing brackets.
    private String readRawTypeParameters() {
      idx = skipSpaces(str, idx);

      if (isEOS() || str.charAt(idx) == '>' || str.charAt(idx) == ',') {
        return "";
      }

      if (str.charAt(idx) != '<') {
        throw new IllegalStateException(
            String.format(
                "Expecting char %d of %s to be '<' but '%c' found", idx, str, str.charAt(idx)));
      }

      int i = idx;
      int open = 1;
      boolean inQuotes = false;
      while (open > 0) {
        ++idx;

        if (isEOS()) {
          throw new IllegalStateException("Non closed angle brackets");
        }

        // Only parse for '<' and '>' characters if not within a quoted identifier.
        // Note we don't need to handle escaped quotes ("") in type names here, because they just
        // cause inQuotes to flip to false and immediately back to true
        if (!inQuotes) {
          if (str.charAt(idx) == '"') {
            inQuotes = true;
          } else if (str.charAt(idx) == '<') {
            open++;
          } else if (str.charAt(idx) == '>') {
            open--;
          }
        } else if (str.charAt(idx) == '"') {
          inQuotes = false;
        }
      }
      // we've stopped at the last closing ')' so move past that
      ++idx;
      return str.substring(i, idx);
    }

    // skip all blank and at best one comma, return true if there not EOS
    private boolean skipBlankAndComma() {
      boolean commaFound = false;
      while (!isEOS()) {
        int c = str.charAt(idx);
        if (c == ',') {
          if (commaFound) {
            return true;
          } else {
            commaFound = true;
          }
        } else if (!isBlank(c)) {
          return true;
        }
        ++idx;
      }
      return false;
    }

    private boolean isEOS() {
      return idx >= str.length();
    }

    // // // Methods from "ParseUtils":

    private static int skipSpaces(String toParse, int idx) {
      while (idx < toParse.length() && isBlank(toParse.charAt(idx))) ++idx;
      return idx;
    }

    private static boolean isBlank(int c) {
      return c == ' ' || c == '\t' || c == '\n';
    }

    public static boolean isCqlIdentifierChar(int c) {
      return (c >= '0' && c <= '9')
          || (c >= 'a' && c <= 'z')
          || (c >= 'A' && c <= 'Z')
          || c == '-'
          || c == '+'
          || c == '.'
          || c == '_'
          || c == '&';
    }
  }
}
