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

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.internal.GrpcUtil;
import io.micrometer.core.instrument.Tags;
import io.stargate.core.metrics.StargateMetricConstants;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Extension of the {@link GrpcMetricsTagProvider} that can extract user agent information from the
 * request metadata.
 *
 * <p>To avoid metric explosion, we are processing the user agent value in the following way:
 *
 * <ol>
 *   <li><code>grpc-java-netty/1.51.0</code> results in <code>grpc-java</code>
 *   <li><code>my-agent/1.0.0 grpc-java-netty/1.51.0</code> results in <code>my-agent/grpc-java
 *       </code>
 *   <li><code>arbitrary-agent-value</code> results in <code>arbitrary-agent-value</code>
 * </ol>
 */
public interface UserAgentTagProvider extends GrpcMetricsTagProvider {

  String USER_AGENT_TAG_KEY = "userAgent";

  /** {@inheritDoc} */
  @Override
  default Tags getCallTags(ServerCall<?, ?> call, Metadata requestHeaders) {
    String userAgent = requestHeaders.get(GrpcUtil.USER_AGENT_KEY);

    // if not defined, return UNKNOWN
    if (null == userAgent) {
      return Tags.of(USER_AGENT_TAG_KEY, StargateMetricConstants.UNKNOWN);
    }

    // otherwise first try to split
    // reason is that user setting the metadata would be
    // my-agent/1.0.0 grpc-java-netty/1.51.0
    String[] agentParts = userAgent.split(" ");
    String tagValue =
        Arrays.stream(agentParts)

            // remove version from agent name
            // input ex. grpc-java-netty/1.51.0, my-agent/1.0.0
            .map(
                agentAndVersion -> {
                  int versionIndex = agentAndVersion.indexOf('/');
                  if (versionIndex > 0) {
                    return agentAndVersion.substring(0, versionIndex);
                  } else {
                    return agentAndVersion;
                  }
                })

            // default grpc lib, remove the server
            // input ex. grpc-java-netty, grpc-node-js, my-agent
            .map(
                agent -> {
                  int last = agent.lastIndexOf('-');
                  // ensure that we never cut to `grpc` only
                  if (agent.startsWith("grpc-") && last > 4) {
                    return agent.substring(0, last);
                  } else {
                    return agent;
                  }
                })

            // join list
            .collect(Collectors.joining("/"));

    return Tags.of(USER_AGENT_TAG_KEY, tagValue);
  }
}
