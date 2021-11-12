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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

public class ByteBufferUtils {

  private ByteBufferUtils() {}

  public static String toBase64(ByteBuffer buffer) {
    return toBase64(getArray(buffer));
  }

  public static String toBase64(byte[] bytes) {
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static ByteBuffer fromBase64(String base64) {
    return ByteBuffer.wrap(Base64.getDecoder().decode(base64));
  }

  public static String toBase64ForUrl(ByteBuffer buffer) {
    return toBase64ForUrl(getArray(buffer));
  }

  public static String toBase64ForUrl(byte[] bytes) {
    return Base64.getUrlEncoder().encodeToString(bytes);
  }

  public static ByteBuffer fromBase64UrlParam(String base64) {
    // TODO: remove support for legacy use cases when they are no longer relevant
    if (isLegacyEncodedBase64String(base64)) {
      // Fix legacy strings that got broken by decoding `+` at HTTP level
      base64 = base64.replace(' ', '+');
      // Use the decoder compatible with the encoder previously used for URL params
      return ByteBuffer.wrap(Base64.getDecoder().decode(base64));
    }

    return ByteBuffer.wrap(Base64.getUrlDecoder().decode(base64));
  }

  private static boolean isLegacyEncodedBase64String(String base64) {
    // From
    // https://cowtowncoder.medium.com/measuring-string-indexofany-string-performance-java-fecb9eb473fa
    // use the fastest (and simple) method here (since commons-lang3 not yet a dependency)
    for (int i = 0, len = base64.length(); i < len; ++i) {
      switch (base64.charAt(i)) {
        case '/':
        case '+':
        case ' ':
          return true;
      }
    }
    return false;
  }

  /**
   * Extract the content of the provided {@code ByteBuffer} as a byte array.
   *
   * <p>This method work with any type of {@code ByteBuffer} (direct and non-direct ones), but when
   * the {@code ByteBuffer} is backed by an array, this method will try to avoid copy when possible.
   * As a consequence, changes to the returned byte array may or may not reflect into the initial
   * {@code ByteBuffer}.
   *
   * @param bytes the buffer whose content to extract.
   * @return a byte array with the content of {@code bytes}. That array may be the array backing
   *     {@code bytes} if this can avoid a copy.
   */
  public static byte[] getArray(ByteBuffer bytes) {
    int length = bytes.remaining();

    if (bytes.hasArray()) {
      int boff = bytes.arrayOffset() + bytes.position();
      if (boff == 0 && length == bytes.array().length) return bytes.array();
      else return Arrays.copyOfRange(bytes.array(), boff, boff + length);
    }
    // else, DirectByteBuffer.get() is the fastest route
    byte[] array = new byte[length];
    bytes.duplicate().get(array);
    return array;
  }
}
