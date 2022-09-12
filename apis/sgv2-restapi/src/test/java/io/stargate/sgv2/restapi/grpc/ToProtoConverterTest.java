package io.stargate.sgv2.restapi.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class ToProtoConverterTest {
  private static final String TEST_TABLE = "test_table";
  private static final String TEST_COLUMN = "test_column";

  private static final ToProtoValueCodecs TO_PROTO_VALUE_CODECS = new ToProtoValueCodecs();

  private static Arguments[] fromExternalSamplesStrict() {
    return new Arguments[] {
      arguments(123, basicType(TypeSpec.Basic.INT), Values.of(123)),
      arguments(-4567L, basicType(TypeSpec.Basic.BIGINT), Values.of(-4567L)),
      arguments("abc", basicType(TypeSpec.Basic.VARCHAR), Values.of("abc")),
      arguments("/w==", basicType(TypeSpec.Basic.BLOB), Values.of(new byte[] {(byte) 0xFF})),
      arguments("3d", basicType(TypeSpec.Basic.DURATION), Values.of(CqlDuration.from("3d"))),

      // Lists, Sets
      arguments(
          Arrays.asList("foo", "bar"),
          listType(TypeSpec.Basic.VARCHAR),
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          Arrays.asList(123, 456),
          listType(TypeSpec.Basic.INT),
          Values.of(Arrays.asList(Values.of(123), Values.of(456)))),
      arguments(
          Arrays.asList("foo", "bar"),
          setType(TypeSpec.Basic.VARCHAR),
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          Arrays.asList(123, 456),
          setType(TypeSpec.Basic.INT),
          Values.of(Arrays.asList(Values.of(123), Values.of(456)))),

      // Maps
      arguments(
          Collections.singletonMap("foo", "bar"),
          mapType(TypeSpec.Basic.VARCHAR, TypeSpec.Basic.VARCHAR),
          // since internal representation is just as Collection...
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          Collections.singletonMap(123, Boolean.TRUE),
          mapType(TypeSpec.Basic.INT, TypeSpec.Basic.BOOLEAN),
          Values.of(Arrays.asList(Values.of(123), Values.of(true)))),
    };
  }
  ;

  @ParameterizedTest
  @MethodSource("fromExternalSamplesStrict")
  @DisplayName("Should coerce 'strict' external value to Bridge/grpc value")
  public void strictExternalToBridgeValueTest(
      Object externalValue, TypeSpec typeSpec, QueryOuterClass.Value bridgeValue) {
    ToProtoConverter conv = createConverter(typeSpec);
    // First verify that it works in strict mode
    assertThat(conv.protoValueFromStrictlyTyped(TEST_COLUMN, externalValue)).isEqualTo(bridgeValue);
    // But also that "loose" accepts it as well
    assertThat(conv.protoValueFromLooselyTyped(TEST_COLUMN, externalValue)).isEqualTo(bridgeValue);
  }

  private static Arguments[] fromExternalSamplesStringified() {
    return new Arguments[] {
      arguments("123", basicType(TypeSpec.Basic.INT), Values.of(123)),
      arguments("-4567", basicType(TypeSpec.Basic.BIGINT), Values.of(-4567L)),
      arguments("abc", basicType(TypeSpec.Basic.VARCHAR), Values.of("abc")),
      arguments("'abc'", basicType(TypeSpec.Basic.VARCHAR), Values.of("abc")),
      arguments(
          "'quoted=''value'''", basicType(TypeSpec.Basic.VARCHAR), Values.of("quoted='value'")),
      arguments("2d", basicType(TypeSpec.Basic.DURATION), Values.of(CqlDuration.from("2d"))),
      arguments("'2d'", basicType(TypeSpec.Basic.DURATION), Values.of(CqlDuration.from("2d"))),

      // Lists, Sets
      arguments(
          "['foo','bar']",
          listType(TypeSpec.Basic.VARCHAR),
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          "[123, 456]",
          listType(TypeSpec.Basic.INT),
          Values.of(Arrays.asList(Values.of(123), Values.of(456)))),
      // Maps
      arguments(
          "{'foo': 'bar'}",
          mapType(TypeSpec.Basic.VARCHAR, TypeSpec.Basic.VARCHAR),
          // since internal representation is just as Collection...
          Values.of(Arrays.asList(Values.of("foo"), Values.of("bar")))),
      arguments(
          "{123:true}",
          mapType(TypeSpec.Basic.INT, TypeSpec.Basic.BOOLEAN),
          Values.of(Arrays.asList(Values.of(123), Values.of(true)))),
    };
  }
  ;

  @ParameterizedTest
  @MethodSource("fromExternalSamplesStringified")
  @DisplayName("Should coerce 'stringified' external value to Bridge/grpc value")
  public void stringifiedExternalToBridgeValueTest(
      String externalValue, TypeSpec typeSpec, QueryOuterClass.Value bridgeValue) {
    ToProtoConverter conv = createConverter(typeSpec);

    // Ensure explicitly Stringified works
    assertThat(conv.protoValueFromStringified(TEST_COLUMN, externalValue)).isEqualTo(bridgeValue);
    // but also general "loose"
    assertThat(conv.protoValueFromLooselyTyped(TEST_COLUMN, externalValue)).isEqualTo(bridgeValue);
  }

  // Test for [stargate#2061]
  @DisplayName("Should be able to create converter for deeply nested type")
  @Test
  public void deeplyNestMapOfListOfTuples() {
    TypeSpec doubleType = basicType(TypeSpec.Basic.DOUBLE);
    TypeSpec tupleType = tupleType(doubleType, doubleType);
    // Let's assert types from innermost to outermost; failure is via exception
    assertThat(createConverter(tupleType)).isNotNull();
    TypeSpec listType = listType(tupleType);
    assertThat(createConverter(listType)).isNotNull();
    TypeSpec mapType = mapType(basicType(TypeSpec.Basic.VARCHAR), listType);
    assertThat(createConverter(mapType)).isNotNull();
  }

  /*
  ///////////////////////////////////////////////////////////////////////
  // Helper methods for constructing scaffolding for Bridge/gRPC
  ///////////////////////////////////////////////////////////////////////
   */

  private static ToProtoConverter createConverter(TypeSpec typeSpec) {
    ColumnSpec column = ColumnSpec.newBuilder().setName(TEST_COLUMN).setType(typeSpec).build();
    ToProtoValueCodec codec = TO_PROTO_VALUE_CODECS.codecFor(column);
    return new ToProtoConverter(TEST_TABLE, Collections.singletonMap(TEST_COLUMN, codec));
  }

  private static TypeSpec basicType(TypeSpec.Basic basicType) {
    return TypeSpec.newBuilder().setBasic(basicType).build();
  }

  private static TypeSpec listType(TypeSpec.Basic basicElementType) {
    return listType(basicType(basicElementType));
  }

  private static TypeSpec listType(TypeSpec elementType) {
    return TypeSpec.newBuilder()
        .setList(TypeSpec.List.newBuilder().setElement(elementType).build())
        .build();
  }

  private static TypeSpec setType(TypeSpec.Basic basicElementType) {
    return setType(basicType(basicElementType));
  }

  private static TypeSpec setType(TypeSpec elementType) {
    return TypeSpec.newBuilder()
        .setSet(TypeSpec.Set.newBuilder().setElement(elementType).build())
        .build();
  }

  private static TypeSpec mapType(TypeSpec.Basic basicKeyType, TypeSpec.Basic basicValueType) {
    return mapType(basicType(basicKeyType), basicType(basicValueType));
  }

  private static TypeSpec mapType(TypeSpec keyType, TypeSpec valueType) {
    return TypeSpec.newBuilder()
        .setMap(TypeSpec.Map.newBuilder().setKey(keyType).setValue(valueType).build())
        .build();
  }

  private static TypeSpec tupleType(TypeSpec... types) {
    TypeSpec.Tuple.Builder tupleBuilder = TypeSpec.Tuple.newBuilder();
    for (TypeSpec elemType : types) {
      tupleBuilder = tupleBuilder.addElements(elemType);
    }
    return TypeSpec.newBuilder().setTuple(tupleBuilder.build()).build();
  }
}
