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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.nio.ByteBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class BytesValuesTest {

  @ParameterizedTest
  @MethodSource("values")
  public void shouldCreateFromBytes(
      byte[] bytes, @SuppressWarnings("unused") String base64, String utf8) {
    BytesValue value = BytesValues.of(bytes);
    assertThat(value.getValue().toStringUtf8()).isEqualTo(utf8);
  }

  @ParameterizedTest
  @MethodSource("values")
  public void shouldCreateFromByteBuffer(
      byte[] bytes, @SuppressWarnings("unused") String base64, String utf8) {
    BytesValue value = BytesValues.of(ByteBuffer.wrap(bytes));
    assertThat(value.getValue().toStringUtf8()).isEqualTo(utf8);
  }

  @ParameterizedTest
  @MethodSource("values")
  public void shouldCreateFromBase64(
      @SuppressWarnings("unused") byte[] bytes, String base64, String utf8) {
    BytesValue value = BytesValues.ofBase64(base64);
    assertThat(value.getValue().toStringUtf8()).isEqualTo(utf8);
  }

  @ParameterizedTest
  @MethodSource("values")
  public void shouldConvertToBytes(
      byte[] bytes, @SuppressWarnings("unused") String base64, String utf8) {
    BytesValue value = BytesValue.of(ByteString.copyFromUtf8(utf8));
    assertThat(BytesValues.toBytes(value)).isEqualTo(bytes);
  }

  @ParameterizedTest
  @MethodSource("values")
  public void shouldConvertToByteBuffer(
      byte[] bytes, @SuppressWarnings("unused") String base64, String utf8) {
    BytesValue value = BytesValue.of(ByteString.copyFromUtf8(utf8));
    assertThat(BytesValues.toByteBuffer(value)).isEqualTo(ByteBuffer.wrap(bytes));
  }

  @ParameterizedTest
  @MethodSource("values")
  public void shouldConvertToBase64(
      @SuppressWarnings("unused") byte[] bytes, String base64, String utf8) {
    BytesValue value = BytesValue.of(ByteString.copyFromUtf8(utf8));
    assertThat(BytesValues.toBase64(value)).isEqualTo(base64);
  }

  public static Arguments[] values() {
    return new Arguments[] {
      arguments(new byte[0], "", ""),
      arguments(
          new byte[] {
            0x61, 0x62, 0x63, 0x64,
          },
          "YWJjZA==",
          "abcd"),
    };
  }
}
