package io.stargate.sgv2.restsvc.grpc;

import io.stargate.proto.QueryOuterClass;
import java.util.stream.Collectors;

public class BridgeProtoTypeConverter {
  public static String fromProtoToCqlType(QueryOuterClass.TypeSpec type) {
    boolean frozen;
    String desc;

    switch (type.getSpecCase()) {
      case BASIC:
        return basicProtoToCqlType(type.getBasic());
      case LIST:
        {
          QueryOuterClass.TypeSpec.List listType = type.getList();
          desc = "list<" + fromProtoToCqlType(listType.getElement()) + ">";
          frozen = listType.getFrozen();
          break;
        }
      case MAP:
        {
          QueryOuterClass.TypeSpec.Map mapType = type.getMap();
          desc =
              "map<"
                  + fromProtoToCqlType(mapType.getKey())
                  + ", "
                  + fromProtoToCqlType(mapType.getValue())
                  + ">";
          frozen = mapType.getFrozen();
          break;
        }
      case SET:
        {
          QueryOuterClass.TypeSpec.Set setType = type.getSet();
          desc = "set<" + fromProtoToCqlType(setType.getElement()) + ">";
          frozen = setType.getFrozen();
          break;
        }
      case TUPLE:
        {
          QueryOuterClass.TypeSpec.Tuple tupleType = type.getTuple();
          String types =
              tupleType.getElementsList().stream()
                  .map(elem -> fromProtoToCqlType(elem))
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
    if (frozen) {
      desc = "frozen<" + desc + ">";
    }
    return desc;
  }

  private static String basicProtoToCqlType(QueryOuterClass.TypeSpec.Basic type) {
    switch (type) {
        // special cases first, if any

        // and then default: lower-case of enum itself
        // NOTE: could be optimized by map lookup if we care
      default:
        return type.name().toLowerCase();
    }
  }
}
