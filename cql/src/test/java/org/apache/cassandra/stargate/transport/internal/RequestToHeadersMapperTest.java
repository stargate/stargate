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
package org.apache.cassandra.stargate.transport.internal;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import io.stargate.db.ClientInfo;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class RequestToHeadersMapperTest {
  @Test
  public void shouldMapInetSocketAddressToHeader() throws UnknownHostException {
    // given
    UUID uuid = UUID.randomUUID();

    ClientInfo clientInfo =
        new ClientInfo(
            null, new InetSocketAddress(InetAddress.getByAddress(getUUIDBytes(uuid)), 1000));

    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.toHeaders(clientInfo);

    //
    assertThat(allHeaders)
        .containsExactly(
            new AbstractMap.SimpleEntry<>(
                RequestToHeadersMapper.TENANT_ID_HEADER_NAME, uuid.toString()));
  }

  @Test
  public void shouldDoesUseThePortFromInetSocketAddressWhenCreatingTenantIdUUID()
      throws UnknownHostException {
    UUID uuid = UUID.randomUUID();

    // given
    ClientInfo clientInfo =
        new ClientInfo(
            null, new InetSocketAddress(Inet6Address.getByAddress(getUUIDBytes(uuid)), 1000));
    ClientInfo clientInfoDifferentPort =
        new ClientInfo(
            null, new InetSocketAddress(Inet6Address.getByAddress(getUUIDBytes(uuid)), 1234));

    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.toHeaders(clientInfo);
    Map<String, String> allHeadersDifferentPort =
        RequestToHeadersMapper.toHeaders(clientInfoDifferentPort);

    //
    assertThat(allHeaders).isEqualTo(allHeadersDifferentPort);
  }

  @Test
  public void shouldNotMapInetSocketAddressIfItsNotPresent() {
    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.toHeaders(new ClientInfo(null, null));

    //
    assertThat(allHeaders).isEmpty();
  }

  @NotNull
  private byte[] getUUIDBytes(UUID uuid) {
    byte[] uuidBytes = new byte[16];
    ByteBuffer bb = ByteBuffer.wrap(uuidBytes);
    bb.putLong(uuid.getMostSignificantBits());
    bb.putLong(uuid.getLeastSignificantBits());
    return uuidBytes;
  }
}
