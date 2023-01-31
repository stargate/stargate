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
import io.micrometer.core.instrument.Tags;

/** No-op implementation of the {@link GrpcMetricsTagProvider}, returns empty tags for any call. */
public class NoopGrpcMetricsTagProvider implements GrpcMetricsTagProvider {

  /** {@inheritDoc} */
  @Override
  public Tags getCallTags(ServerCall<?, ?> call, Metadata requestHeaders) {
    return Tags.empty();
  }
}
