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
package io.stargate.db;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.AbstractComparableAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClientInfoTest {

  private static final InetSocketAddress localhost =
      InetSocketAddress.createUnresolved("localhost", 1);

  private ClientInfo userInfo;
  private ClientInfo emptyInfo;

  @BeforeEach
  public void setup() {
    emptyInfo = new ClientInfo(localhost, localhost);
    userInfo = new ClientInfo(localhost, localhost);
    userInfo.setAuthenticatedUser(
        AuthenticatedUser.of(
            "user123", "token123", true, Collections.singletonMap("customProp1", "customValue1")));
  }

  private AbstractStringAssert<?> assertStringValue(ClientInfo clientInfo, String key) {
    Map<String, ByteBuffer> payload = new HashMap<>();
    clientInfo.storeAuthenticationData(payload);
    return assertThat(UTF_8.decode(payload.get(key)).toString());
  }

  private AbstractComparableAssert<?, ByteBuffer> assertBufferValue(
      ClientInfo clientInfo, String key) {
    Map<String, ByteBuffer> payload = new HashMap<>();
    clientInfo.storeAuthenticationData(payload);
    return assertThat(payload.get(key));
  }

  @Test
  public void getToken() {
    assertStringValue(userInfo, "stargate.auth.subject.token").isEqualTo("token123");
  }

  @Test
  public void getRoleName() {
    assertStringValue(userInfo, "stargate.auth.subject.role").isEqualTo("user123");
  }

  @Test
  public void getIsFromExternalAuth() {
    assertBufferValue(userInfo, "stargate.auth.subject.fromExternalAuth").isNotNull();
  }

  @Test
  public void getCustom() {
    assertStringValue(userInfo, "stargate.auth.subject.custom.customProp1")
        .isEqualTo("customValue1");
  }

  @Test
  public void emptyUser() {
    assertBufferValue(emptyInfo, "stargate.auth.subject.token").isNull();
    assertBufferValue(emptyInfo, "stargate.auth.subject.role").isNull();
    assertBufferValue(emptyInfo, "stargate.auth.subject.external").isNull();
  }

  @Test
  public void testUserInfoBufferReusable() throws Exception {
    // decode twice to ensure the stored buffer is reusable
    for (int i = 0; i < 2; i++) {
      Map<String, ByteBuffer> payload = new HashMap<>();
      userInfo.storeAuthenticationData(payload);

      for (ByteBuffer buffer : payload.values()) {
        assertThat(UTF_8.decode(buffer).remaining()).isGreaterThan(0);

        // ensure the buffer is read-only
        assertThat(buffer.isReadOnly()).isTrue();
      }
    }
  }

  @Test
  public void toStringIsNullSafe() {
    assertDoesNotThrow(new ClientInfo(null, null)::toString);
  }
}
