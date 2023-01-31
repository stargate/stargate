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

package io.stargate.testing.metrics;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.micrometer.core.instrument.Tags;
import io.stargate.grpc.metrics.api.GrpcMetricsTagProvider;
import java.util.Optional;

/** Adds {@value AUTHORITY_KEY} as tag based on the gRPC call#getAuthority. */
public class AuthorityGrpcMetricsTagProvider implements GrpcMetricsTagProvider {

  public static final String AUTHORITY_KEY = "authority";

  @Override
  public Tags getCallTags(ServerCall<?, ?> call, Metadata requestHeaders) {
    return Optional.ofNullable(call.getAuthority())
        .map(a -> Tags.of(AUTHORITY_KEY, a))
        .orElseGet(() -> Tags.of(AUTHORITY_KEY, "UNKNOWN"));
  }
}
