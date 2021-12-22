package io.stargate.sgv2.restsvc.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import java.util.Collections;
import org.junit.jupiter.api.DisplayName;
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
      arguments("abc", basicType(TypeSpec.Basic.TEXT), Values.of("abc")),
      arguments("/w==", basicType(TypeSpec.Basic.BLOB), Values.of(new byte[] {(byte) 0xFF})),
    };
  };

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
      arguments("abc", basicType(TypeSpec.Basic.TEXT), Values.of("abc")),
      arguments("'abc'", basicType(TypeSpec.Basic.TEXT), Values.of("abc")),
      arguments("'quoted=''value'''", basicType(TypeSpec.Basic.TEXT), Values.of("quoted='value'")),
    };
  };

  @ParameterizedTest
  @MethodSource("fromExternalSamplesStringified")
  @DisplayName("Should coerce external value to Bridge/grpc value")
  public void stringifiedExternalToBridgeValueTest(
      String externalValue, TypeSpec typeSpec, QueryOuterClass.Value bridgeValue) {
    ToProtoConverter conv = createConverter(typeSpec);

    // Ensure explicitly Stringified works
    assertThat(conv.protoValueFromStringified(TEST_COLUMN, externalValue)).isEqualTo(bridgeValue);
    // but also general "loose"
    assertThat(conv.protoValueFromLooselyTyped(TEST_COLUMN, externalValue)).isEqualTo(bridgeValue);
  }

  /*
  ///////////////////////////////////////////////////////////////////////
  // Helper methods for constructing scaffolding for Bridge/gRPC
  ///////////////////////////////////////////////////////////////////////
   */

  private static ToProtoConverter createConverter(TypeSpec typeSpec) {
    ColumnSpec column = ColumnSpec.newBuilder().setName("testColumn").setType(typeSpec).build();
    ToProtoValueCodec codec = TO_PROTO_VALUE_CODECS.codecFor(column);
    return new ToProtoConverter(TEST_TABLE, Collections.singletonMap(TEST_COLUMN, codec));
  }

  private static TypeSpec basicType(TypeSpec.Basic basicType) {
    return TypeSpec.newBuilder().setBasic(basicType).build();
  }
}
