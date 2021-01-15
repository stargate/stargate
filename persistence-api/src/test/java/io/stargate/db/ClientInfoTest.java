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

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ClientInfoTest {

  private static InetSocketAddress localhost = InetSocketAddress.createUnresolved("localhost", 1);

  private ClientInfo userInfo;
  private ClientInfo emptyInfo;

  @BeforeEach
  public void setup() {
    emptyInfo = new ClientInfo(localhost, localhost);
    userInfo = new ClientInfo(localhost, localhost);
    userInfo.setAuthenticatedUser(AuthenticatedUser.of("user123", "token123", true));
  }

  @Test
  public void getToken() {
    assertThat(UTF_8.decode(userInfo.getToken()).toString()).isEqualTo("token123");
    assertThat(emptyInfo.getToken()).isNull();
  }

  @Test
  public void getRoleName() {
    assertThat(UTF_8.decode(userInfo.getRoleName()).toString()).isEqualTo("user123");
    assertThat(emptyInfo.getRoleName()).isNull();
  }

  @Test
  public void getIsFromExternalAuth() {
    assertThat(userInfo.getIsFromExternalAuth()).isNotNull();
    assertThat(emptyInfo.getIsFromExternalAuth()).isNull();
  }

  @ParameterizedTest
  @CsvSource({"getToken", "getRoleName", "getIsFromExternalAuth"})
  public void testUserInfoBufferReusable(String methodName) throws Exception {
    Method method = userInfo.getClass().getMethod(methodName);

    // decode twice to ensure the stored buffer is reusable
    assertThat(UTF_8.decode((ByteBuffer) method.invoke(userInfo)).remaining()).isGreaterThan(0);
    assertThat(UTF_8.decode((ByteBuffer) method.invoke(userInfo)).remaining()).isGreaterThan(0);

    // ensure the buffer is read-only
    assertThat(((ByteBuffer) method.invoke(userInfo)).isReadOnly()).isTrue();
  }
}
