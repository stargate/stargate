package io.stargate.sgv2.restsvc.grpc;

import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class ToProtoConverterTest {
    private final ToProtoValueCodecs TO_PROTO_VALUE_CODECS = new ToProtoValueCodecs();

    private static Arguments[] fromExternalSamplesStrict() {
        return new Arguments[] {
           arguments(123, basicType(TypeSpec.Basic.INT), Values.of(123)),
           arguments(-4567L, basicType(TypeSpec.Basic.BIGINT), Values.of(-4567L)),
           arguments("abc", basicType(TypeSpec.Basic.TEXT), Values.of("abc")),

           arguments("/w==", basicType(TypeSpec.Basic.BLOB), Values.of(new byte[] { (byte) 0xFF })),
        };
    };

    @ParameterizedTest
    @MethodSource("fromExternalSamplesStrict")
    @DisplayName("Should coerce 'strict' external value to Bridge/grpc value")
    public void strictExternalToBridgeValueTest(Object externalValue, TypeSpec typeSpec,
                                          QueryOuterClass.Value bridgeValue) throws Exception {
        ColumnSpec column = ColumnSpec.newBuilder()
                .setName("testColumn")
                .setType(typeSpec)
                .build();

        ToProtoValueCodec codec = TO_PROTO_VALUE_CODECS.codecFor(column);
        assertThat(codec).isNotNull();

        // First verify that it works in strict mode
        assertThat(codec.protoValueFromStrictlyTyped(externalValue))
                .isEqualTo(bridgeValue);
        // But also that "loose" accepts it as well
        assertThat(codec.protoValueFromLooselyTyped(externalValue))
                .isEqualTo(bridgeValue);
    }

    private static Arguments[] fromExternalSamplesStringified() {
        return new Arguments[] {
                arguments("123", basicType(TypeSpec.Basic.INT), Values.of(123)),
                arguments("-4567", basicType(TypeSpec.Basic.BIGINT), Values.of(-4567L)),
                arguments("abc", basicType(TypeSpec.Basic.TEXT), Values.of("abc")),
                arguments("'abc'", basicType(TypeSpec.Basic.TEXT), Values.of("abc")),
        };
    };

    @ParameterizedTest
    @MethodSource("fromExternalSamplesStringified")
    @DisplayName("Should coerce external value to Bridge/grpc value")
    public void stringifiedExternalToBridgeValueTest(String externalValue, TypeSpec typeSpec,
                                                QueryOuterClass.Value bridgeValue) throws Exception {
        ColumnSpec column = ColumnSpec.newBuilder()
                .setName("testColumn")
                .setType(typeSpec)
                .build();
        ToProtoValueCodec codec = TO_PROTO_VALUE_CODECS.codecFor(column);
        assertThat(codec).isNotNull();

        // Ensure explicitly Stringified works
        assertThat(codec.protoValueFromStringified(externalValue))
                .isEqualTo(bridgeValue);
        // but also general "loose"
        assertThat(codec.protoValueFromLooselyTyped(externalValue))
                .isEqualTo(bridgeValue);

        // Further, allow additional apostrophe quoting as well
    }

    private static TypeSpec basicType(TypeSpec.Basic basicType) {
        return TypeSpec.newBuilder().setBasic(basicType).build();
    }
}

