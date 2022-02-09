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
package io.stargate.sgv2.common.grpc;

import io.grpc.Channel;
import java.util.Optional;

class DefaultStargateBridgeClientFactory implements StargateBridgeClientFactory {

  private final Channel channel;
  private final DefaultStargateBridgeSchema schema;

  DefaultStargateBridgeClientFactory(Channel channel, String adminAuthToken) {
    this.channel = channel;
    this.schema = new DefaultStargateBridgeSchema(channel, adminAuthToken);
  }

  @Override
  public StargateBridgeClient newClient(String authToken, Optional<String> tenantId) {
    return new DefaultStargateBridgeClient(channel, schema, authToken, tenantId);
  }

  @Override
  public StargateBridgeSchema getSchema() {
    return schema;
  }
}
