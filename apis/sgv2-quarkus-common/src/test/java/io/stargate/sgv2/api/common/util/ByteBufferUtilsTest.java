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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.api.common.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * @author Dmitri Bourlatchkov
 */
class ByteBufferUtilsTest {

  @Nested
  class ToBase64ForUrl {

    @Test
    void roundTrip() {
      for (byte i = Byte.MIN_VALUE; i < Byte.MAX_VALUE; i++) {
        for (byte j = Byte.MIN_VALUE; j < Byte.MAX_VALUE; j++) {
          byte[] data = {i, j};
          String str = ByteBufferUtils.toBase64ForUrl(ByteBuffer.wrap(data));
          assertThat(str).doesNotContain("+");
          assertThat(str).doesNotContain("/");
          assertThat(ByteBufferUtils.fromBase64UrlParam(str).array()).isEqualTo(data);
        }
      }
    }
  }
}
