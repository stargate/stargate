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

import io.stargate.db.ClientInfo;
import java.nio.ByteBuffer;
import java.util.*;

public class RequestToHeadersMapper {
  public static final String TENANT_ID_HEADER_NAME = "tenant_id";

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static Map<String, String> toHeaders(ClientInfo clientInfo) {
    Map<String, String> tenantIdHeaders = new HashMap<>();
    clientInfo
        .publicAddress()
        .map(tenantIdAddress -> toUUID(tenantIdAddress.getAddress().getAddress()))
        .ifPresent(
            tenantIdUUID -> tenantIdHeaders.put(TENANT_ID_HEADER_NAME, tenantIdUUID.toString()));
    return tenantIdHeaders;
  }

  private static UUID toUUID(byte[] addressBytes) {
    ByteBuffer bytes = ByteBuffer.wrap(addressBytes);
    return new UUID(bytes.getLong(), bytes.getLong());
  }
}
