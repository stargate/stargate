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

import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import java.nio.ByteBuffer;
import java.util.Base64;

public class BytesValues {

  public static BytesValue of(byte[] bytes) {
    return BytesValue.of(ByteString.copyFrom(bytes));
  }

  public static BytesValue of(ByteBuffer byteBuffer) {
    return BytesValue.of(ByteString.copyFrom(byteBuffer));
  }

  public static BytesValue ofBase64(String base64) {
    return of(Base64.getDecoder().decode(base64));
  }

  public static byte[] toBytes(BytesValue value) {
    return value.getValue().toByteArray();
  }

  public static ByteBuffer toByteBuffer(BytesValue value) {
    return value.getValue().asReadOnlyByteBuffer();
  }

  public static String toBase64(BytesValue value) {
    return Base64.getEncoder().encodeToString(toBytes(value));
  }

  private BytesValues() {
    // intentionally empty
  }
}
