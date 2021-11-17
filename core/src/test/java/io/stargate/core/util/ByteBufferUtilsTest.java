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
package io.stargate.core.util;

import static io.stargate.core.util.ByteBufferUtils.fromBase64UrlParam;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ByteBufferUtilsTest {

  @Test
  void testUrlRoundTrip() {
    for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
      for (byte j = Byte.MIN_VALUE; j < Byte.MAX_VALUE; j++) {
        byte[] data = {i, j};
        String str = ByteBufferUtils.toBase64ForUrl(ByteBuffer.wrap(data));
        assertThat(str).doesNotContain("+");
        assertThat(str).doesNotContain("/");
        assertThat(fromBase64UrlParam(str).array()).isEqualTo(data);
      }
    }
  }

  // Tests for earlier issue #1041, obsoleted by PR #1405:
  /*
  @Test
  void testLegacyUrlParameterDecoding() {
    assertThat(fromBase64UrlParam("g/A=").array()).isEqualTo(new byte[] {-125, -16});
    assertThat(fromBase64UrlParam("y+A=").array()).isEqualTo(new byte[] {-53, -32});
  }

  @Test
  void testCorruptedLegacyUrlParameterDecoding() {
    // validate the decoding of base64 strings where `+` got replaced by ` ` at the HTTP level
    assertThat(fromBase64UrlParam("y A=").array()).isEqualTo(new byte[] {-53, -32});
  }
  */
}
