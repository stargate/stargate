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

public interface GrpcMetricsTagProvider {

  /**
   * Returns tags for server side gRPC call.
   *
   * <p><b>IMPORTANT:</b> that the implementation must return constant amount of tags for any input.
   * Prometheus does not allow different tags from the single process.
   *
   * @param call Server-side gRPC call
   * @param requestHeaders Call metadata
   * @return Tags
   */
  Tags getCallTags(ServerCall<?, ?> call, Metadata requestHeaders);
}
