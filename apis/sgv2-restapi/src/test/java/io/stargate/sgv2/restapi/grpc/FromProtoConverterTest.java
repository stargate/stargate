package io.stargate.sgv2.restapi.grpc;

import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class FromProtoConverterTest {
  private static final String TEST_COLUMN = "test_column";

  private static final FromProtoValueCodecs FROM_PROTO_VALUE_CODECS = new FromProtoValueCodecs();

  private static Arguments[] fromExternalSamples() {
    return new Arguments[] {
      arguments(123, basicType(QueryOuterClass.TypeSpec.Basic.INT), Values.of(123)),
      arguments(-4567L, basicType(QueryOuterClass.TypeSpec.Basic.BIGINT), Values.of(-4567L)),
      arguments("abc", basicType(QueryOuterClass.TypeSpec.Basic.VARCHAR), Values.of("abc")),
      // Binary data is exposes as byte[] by Converter, not Base64-encoded
      arguments(
          new byte[] {(byte) 0xFF},
          basicType(QueryOuterClass.TypeSpec.Basic.BLOB),
          Values.of(new byte[] {(byte) 0xFF})),
      arguments(
          "3d",
          basicType(QueryOuterClass.TypeSpec.Basic.DURATION),
          Values.of(CqlDuration.from("3d"))),

      // Lists, Sets
      arguments(
          Arrays.asList("foo", "bar"),
          listType(QueryOuterClass.TypeSpec.Basic.VARCHAR),
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          Arrays.asList(123, 456),
          listType(QueryOuterClass.TypeSpec.Basic.INT),
          Values.of(Arrays.asList(Values.of(123), Values.of(456)))),
      arguments(
          setOf("foo", "bar"),
          setType(QueryOuterClass.TypeSpec.Basic.VARCHAR),
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          setOf(123, 456),
          setType(QueryOuterClass.TypeSpec.Basic.INT),
          Values.of(Arrays.asList(Values.of(123), Values.of(456)))),
    };
  }

  private static Arguments[] fromExternalMapSamples() {
    return new Arguments[] {
      // Maps
      arguments(
          false,
          Collections.singletonList(
              new LinkedHashMap<>() {
                {
                  put("key", "foo");
                  put("value", "bar");
                }
              }),
          mapType(QueryOuterClass.TypeSpec.Basic.VARCHAR, QueryOuterClass.TypeSpec.Basic.VARCHAR),
          // since internal representation is just as Collection...
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          true,
          Collections.singletonMap("foo", "bar"),
          mapType(QueryOuterClass.TypeSpec.Basic.VARCHAR, QueryOuterClass.TypeSpec.Basic.VARCHAR),
          // since internal representation is just as Collection...
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          false,
          Arrays.asList(
              new LinkedHashMap<>() {
                {
                  put("key", 123);
                  put("value", true);
                }
              },
              new LinkedHashMap<>() {
                {
                  put("key", 456);
                  put("value", false);
                }
              }),
          mapType(QueryOuterClass.TypeSpec.Basic.INT, QueryOuterClass.TypeSpec.Basic.BOOLEAN),
          Values.of(
              Arrays.asList(Values.of(123), Values.of(true), Values.of(456), Values.of(false)))),
      arguments(
          true,
          new LinkedHashMap<>() {
            {
              put(123, true);
              put(456, false);
            }
          },
          mapType(QueryOuterClass.TypeSpec.Basic.INT, QueryOuterClass.TypeSpec.Basic.BOOLEAN),
          Values.of(
              Arrays.asList(Values.of(123), Values.of(true), Values.of(456), Values.of(false))))
    };
  }

  @ParameterizedTest
  @MethodSource("fromExternalSamples")
  @DisplayName("Should converted Bridge/gRPC value into expected external representation")
  public void strictExternalToBridgeValueTest(
      Object externalValue, QueryOuterClass.TypeSpec typeSpec, QueryOuterClass.Value bridgeValue) {
    FromProtoConverter conv = createConverter(typeSpec, false);
    Map<String, Object> result = conv.mapFromProtoValues(Arrays.asList(bridgeValue));

    assertThat(result.get(TEST_COLUMN)).isEqualTo(externalValue);
  }

  @ParameterizedTest
  @MethodSource("fromExternalMapSamples")
  @DisplayName("Should converted Bridge/gRPC value into expected external representation")
  public void strictExternalToBridgeValueTest(
      boolean optimizeMapData,
      Object externalValue,
      QueryOuterClass.TypeSpec typeSpec,
      QueryOuterClass.Value bridgeValue) {
    FromProtoConverter conv = createConverter(typeSpec, optimizeMapData);
    Map<String, Object> result = conv.mapFromProtoValues(Arrays.asList(bridgeValue));

    assertThat(result.get(TEST_COLUMN)).isEqualTo(externalValue);
  }

  // For [stargate#2246]: handle conversion for "missing" Tuple value
  @Test
  public void emptyOrMissingTupleToNullTest() {
    QueryOuterClass.TypeSpec textType = basicType(QueryOuterClass.TypeSpec.Basic.VARCHAR);
    QueryOuterClass.TypeSpec.Tuple.Builder tupleBuilder =
        QueryOuterClass.TypeSpec.Tuple.newBuilder().addElements(textType).addElements(textType);
    QueryOuterClass.TypeSpec tupleType =
        QueryOuterClass.TypeSpec.newBuilder().setTuple(tupleBuilder.build()).build();
    FromProtoConverter conv = createConverter(tupleType, false);

    // Tuples are represents as Lists
    QueryOuterClass.Value emptyTuple =
        QueryOuterClass.Value.newBuilder()
            .setCollection(QueryOuterClass.Collection.newBuilder())
            .build();
    Map<String, Object> result = conv.mapFromProtoValues(Arrays.asList(emptyTuple));
    assertThat(result.get(TEST_COLUMN)).isNull();
  }

  /*
  ///////////////////////////////////////////////////////////////////////
  // Helper methods for constructing scaffolding for Bridge/gRPC
  ///////////////////////////////////////////////////////////////////////
   */

  private static Set<Object> setOf(Object... values) {
    LinkedHashSet<Object> set = new LinkedHashSet<>();
    set.addAll(Arrays.asList(values));
    return set;
  }

  private static FromProtoConverter createConverter(
      QueryOuterClass.TypeSpec typeSpec, boolean optimizeMapData) {
    QueryOuterClass.ColumnSpec column =
        QueryOuterClass.ColumnSpec.newBuilder().setName(TEST_COLUMN).setType(typeSpec).build();
    FromProtoValueCodec codec = FROM_PROTO_VALUE_CODECS.codecFor(column, optimizeMapData);
    return FromProtoConverter.construct(
        new String[] {TEST_COLUMN}, new FromProtoValueCodec[] {codec});
  }

  private static QueryOuterClass.TypeSpec basicType(QueryOuterClass.TypeSpec.Basic basicType) {
    return QueryOuterClass.TypeSpec.newBuilder().setBasic(basicType).build();
  }

  private static QueryOuterClass.TypeSpec listType(
      QueryOuterClass.TypeSpec.Basic basicElementType) {
    return listType(basicType(basicElementType));
  }

  private static QueryOuterClass.TypeSpec listType(QueryOuterClass.TypeSpec elementType) {
    return QueryOuterClass.TypeSpec.newBuilder()
        .setList(QueryOuterClass.TypeSpec.List.newBuilder().setElement(elementType).build())
        .build();
  }

  private static QueryOuterClass.TypeSpec setType(QueryOuterClass.TypeSpec.Basic basicElementType) {
    return setType(basicType(basicElementType));
  }

  private static QueryOuterClass.TypeSpec setType(QueryOuterClass.TypeSpec elementType) {
    return QueryOuterClass.TypeSpec.newBuilder()
        .setSet(QueryOuterClass.TypeSpec.Set.newBuilder().setElement(elementType).build())
        .build();
  }

  private static QueryOuterClass.TypeSpec mapType(
      QueryOuterClass.TypeSpec.Basic basicKeyType, QueryOuterClass.TypeSpec.Basic basicValueType) {
    return mapType(basicType(basicKeyType), basicType(basicValueType));
  }

  private static QueryOuterClass.TypeSpec mapType(
      QueryOuterClass.TypeSpec keyType, QueryOuterClass.TypeSpec valueType) {
    return QueryOuterClass.TypeSpec.newBuilder()
        .setMap(
            QueryOuterClass.TypeSpec.Map.newBuilder().setKey(keyType).setValue(valueType).build())
        .build();
  }
}
