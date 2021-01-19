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
package io.stargate.db.cassandra.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RequestToHeadersMapper {
  public static final String TENANT_ID_HEADER_NAME = "tenant_id";

  public static Map<String, String> toHeaders(ByteBuffer publicAddress) {
    Map<String, String> tenantIdHeaders = new HashMap<>();
    tenantIdHeaders.put(TENANT_ID_HEADER_NAME, toUUID(publicAddress).toString());
    return tenantIdHeaders;
  }

  private static UUID toUUID(ByteBuffer byteBuf) {
    // the raw bytes of the IPv6 address are the UUID
    return new UUID(byteBuf.getLong(), byteBuf.getLong());
  }
}
