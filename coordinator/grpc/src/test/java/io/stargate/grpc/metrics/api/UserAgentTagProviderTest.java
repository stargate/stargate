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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.grpc.metrics.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Metadata;
import io.grpc.internal.GrpcUtil;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UserAgentTagProviderTest {

  UserAgentTagProvider tagProvider = new UserAgentTagProvider() {};

  @Nested
  class GetCallTags {

    @Test
    public void grpc() {
      Metadata metadata = new Metadata();
      metadata.put(GrpcUtil.USER_AGENT_KEY, "grpc-java-netty/1.0.0");

      Tags result = tagProvider.getCallTags(null, metadata);

      assertThat(result).containsOnlyOnce(Tag.of("user_agent", "grpc-java-netty"));
    }

    @Test
    public void grpcWithoutLib() {
      Metadata metadata = new Metadata();
      metadata.put(GrpcUtil.USER_AGENT_KEY, "grpc-java/1.0.0");

      Tags result = tagProvider.getCallTags(null, metadata);

      assertThat(result).containsOnlyOnce(Tag.of("user_agent", "grpc-java"));
    }

    @Test
    public void customAgent() {
      Metadata metadata = new Metadata();
      metadata.put(GrpcUtil.USER_AGENT_KEY, "user-agent/2.0.0 grpc-java-netty/1.0.0");

      Tags result = tagProvider.getCallTags(null, metadata);

      assertThat(result).containsOnlyOnce(Tag.of("user_agent", "user-agent"));
    }

    @Test
    public void customAgentWithoutVersion() {
      Metadata metadata = new Metadata();
      metadata.put(GrpcUtil.USER_AGENT_KEY, "user-agent grpc-java-netty/1.0.0");

      Tags result = tagProvider.getCallTags(null, metadata);

      assertThat(result).containsOnlyOnce(Tag.of("user_agent", "user-agent"));
    }

    @Test
    public void arbitraryValue() {
      Metadata metadata = new Metadata();
      metadata.put(GrpcUtil.USER_AGENT_KEY, "some-arbitrary-value");

      Tags result = tagProvider.getCallTags(null, metadata);

      assertThat(result).containsOnlyOnce(Tag.of("user_agent", "some-arbitrary-value"));
    }

    @Test
    public void unknown() {
      Metadata metadata = new Metadata();

      Tags result = tagProvider.getCallTags(null, metadata);

      assertThat(result).containsOnlyOnce(Tag.of("user_agent", "unknown"));
    }
  }
}
