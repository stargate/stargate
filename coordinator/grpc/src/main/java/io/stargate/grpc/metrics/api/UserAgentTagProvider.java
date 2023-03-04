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
import java.util.regex.Pattern;

/**
 * Extension of the {@link GrpcMetricsTagProvider} that can extract user agent information from the
 * request metadata.
 *
 * <p>To avoid metric explosion, we are processing the user agent value in the following way:
 *
 * <ol>
 *   <li><code>grpc-java-netty/1.51.0</code> results in <code>grpc-java</code>
 *   <li><code>my-agent/1.0.0 grpc-java-netty/1.51.0</code> results in <code>my-agent</code>
 *   <li><code>arbitrary-agent-value</code> results in <code>arbitrary-agent-value</code>
 * </ol>
 */
public interface UserAgentTagProvider extends GrpcMetricsTagProvider {

  // split pattern for the user agent, extract only first part of the agent
  Pattern USER_AGENT_SPLIT = Pattern.compile("[\\s/]");
  // tag key
  String USER_AGENT_TAG_KEY = "user_agent";

  /** {@inheritDoc} */
  @Override
  default Tags getCallTags(ServerCall<?, ?> call, Metadata requestHeaders) {
    String userAgent = requestHeaders.get(GrpcUtil.USER_AGENT_KEY);
    if (null != userAgent && userAgent.length() > 0) {
      String[] split = USER_AGENT_SPLIT.split(userAgent);
      if (split.length > 0) {
        return Tags.of(USER_AGENT_TAG_KEY, split[0]);
      } else {
        return Tags.of(USER_AGENT_TAG_KEY, userAgent);
      }
    } else {
      return Tags.of(USER_AGENT_TAG_KEY, StargateMetricConstants.UNKNOWN);
    }
  }
}
