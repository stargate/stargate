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

package io.stargate.sgv2.api.common.config;

import io.grpc.Status;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.api.common.grpc.RetriableStargateBridge;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

/** Configuration for the gRPC calls to the Bridge. */
@ConfigMapping(prefix = "stargate.grpc")
public interface GrpcConfig {

  /** @return Optional deadline duration for the each RPC to the bridge. Defaults to 30 seconds. */
  @WithDefault("PT30S")
  Optional<Duration> callDeadline();

  /** @return Defines retry strategy for bridge calls when using {@link RetriableStargateBridge}. */
  @Valid
  @NotNull
  Retries retries();

  interface Retries {

    /** @return If call retries are enabled. */
    @WithDefault("true")
    boolean enabled();

    /**
     * What type of retry policy to use.
     *
     * <ol>
     *   <li><code>status-codes</code> - based on status codes (see {@link
     *       io.stargate.sgv2.api.common.grpc.retries.impl.StatusCodesRetryPredicate}}
     *   <li><code>custom</code> - allows configuring custom policy
     * </ol>
     *
     * If unset, noop policy will be used (never retries).
     *
     * @return The type of the {@link io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate}
     *     used.
     */
    @WithDefault("status-codes")
    @NotNull
    @Pattern(regexp = "status-codes|custom")
    String policy();

    /**
     * @return Set of status codes to execute retries for. Defaults to <code>UNAVAILABLE</code>, as
     *     this code means that the request never reached the bridge or C* responded with
     *     Unavailable exception, thus it should be safe to retry.
     */
    @WithDefault("UNAVAILABLE")
    @NotNull
    Set<Status.Code> statusCodes();

    /** @return Maximum amount of retry attempts. */
    @WithDefault("1")
    @Positive
    int maxAttempts();
  }
}
