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
package org.apache.cassandra.stargate;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RequestToHeadersMapperTest {
  @Test
  public void shouldMapInetSocketAddressToHeader() throws UnknownHostException {
    // given
    Optional<InetSocketAddress> inetSocketAddress =
        Optional.of(new InetSocketAddress(Inet6Address.getByName("::1"), 1000));

    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.toHeaders(inetSocketAddress);

    //
    assertThat(allHeaders)
        .containsExactly(
            new AbstractMap.SimpleEntry<>(
                RequestToHeadersMapper.TENANT_ID_HEADER_NAME,
                "cf404dc8-0617-3c24-9b5b-4fe2531e6d8c"));
  }

  @Test
  public void shouldNotMapInetSocketAddressIfItsNotPresent() {
    // given
    Optional<InetSocketAddress> inetSocketAddress = Optional.empty();

    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.toHeaders(inetSocketAddress);

    //
    assertThat(allHeaders).isEmpty();
  }
}
