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

import java.net.InetSocketAddress;
import java.util.*;
import javax.servlet.http.HttpServletRequest;

public class RequestToHeadersMapper {
  public static final String TENANT_ID_HEADER_NAME = "tenant_id";

  public static Map<String, String> getAllHeaders(HttpServletRequest request) {
    if (request == null) {
      return Collections.emptyMap();
    }
    Map<String, String> allHeaders = new HashMap<>();
    Enumeration<String> headerNames = request.getHeaderNames();
    while (headerNames.hasMoreElements()) {
      String headerName = headerNames.nextElement();
      allHeaders.put(headerName, request.getHeader(headerName));
    }
    return allHeaders;
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static Map<String, String> toHeaders(
      Optional<InetSocketAddress> sourceAddressWithEncodedTenantId) {
    Map<String, String> tenantIdHeaders = new HashMap<>();
    sourceAddressWithEncodedTenantId
        .map(tenantIdAddress -> UUID.nameUUIDFromBytes(tenantIdAddress.getAddress().getAddress()))
        .ifPresent(
            tenantIdUUID -> tenantIdHeaders.put(TENANT_ID_HEADER_NAME, tenantIdUUID.toString()));
    return tenantIdHeaders;
  }
}
