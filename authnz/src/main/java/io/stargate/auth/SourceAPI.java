/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.auth;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public enum SourceAPI {
  GRAPHQL("graphql"),
  CQL("cql"),
  REST("rest");

  public static final String CUSTOM_PAYLOAD_KEY = "stargate.sourceAPI";
  private final String name;

  SourceAPI(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void toCustomPayload(Map<String, ByteBuffer> customPayload) {
    if (null == customPayload) {
      return;
    }

    ByteBuffer buffer = StandardCharsets.UTF_8.encode(this.getName()).asReadOnlyBuffer();
    customPayload.put(CUSTOM_PAYLOAD_KEY, buffer);
  }

  public static SourceAPI fromCustomPayload(Map<String, ByteBuffer> customPayload, SourceAPI defaultIfMissing) {
    if (null == customPayload) {
      return defaultIfMissing;
    }

    return Optional.ofNullable(customPayload.get(CUSTOM_PAYLOAD_KEY))
            .flatMap(buffer -> {
              String decode = StandardCharsets.UTF_8.decode(buffer).toString();
              return Arrays.stream(SourceAPI.values()).filter(s -> Objects.equals(s.getName(), decode)).findFirst();
            })
            .orElse(defaultIfMissing);
  }

}
