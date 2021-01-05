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
package io.stargate.core;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;

class RequestToHeadersMapperTest {

  @Test
  public void shouldGetAllHeadersFromTheHttpServletRequest() {
    // given
    HttpServletRequest httpServletRequest = mock(HttpServletRequest.class);
    when(httpServletRequest.getHeaderNames())
        .thenReturn(Collections.enumeration(Arrays.asList("a", "b", "c")));
    when(httpServletRequest.getHeader("a")).thenReturn("value_a");
    when(httpServletRequest.getHeader("b")).thenReturn("123");
    when(httpServletRequest.getHeader("c")).thenReturn(null);

    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.getAllHeaders(httpServletRequest);

    // then
    assertThat(allHeaders)
        .containsExactly(
            new AbstractMap.SimpleEntry<>("a", "value_a"),
            new AbstractMap.SimpleEntry<>("b", "123"),
            new AbstractMap.SimpleEntry<>("c", null));
  }

  @Test
  public void shouldMapInetSocketAddressToHeader() throws UnknownHostException {
    // given
    Optional<InetSocketAddress> inetSocketAddress =
        Optional.of(new InetSocketAddress(Inet6Address.getLocalHost(), 1000));

    // when
    Map<String, String> allHeaders = RequestToHeadersMapper.toHeaders(inetSocketAddress);

    //
    assertThat(allHeaders)
        .containsExactly(
            new AbstractMap.SimpleEntry<>(
                RequestToHeadersMapper.TENANT_ID_HEADER_NAME,
                "ab930ac7-4e8a-31a5-ae63-fa9aafda6dd5"));
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
