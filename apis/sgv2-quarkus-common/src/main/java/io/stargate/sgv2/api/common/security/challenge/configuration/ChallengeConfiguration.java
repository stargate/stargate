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

package io.stargate.sgv2.api.common.security.challenge.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.api.common.config.AuthConfig;
import io.stargate.sgv2.api.common.security.challenge.ChallengeSender;
import io.stargate.sgv2.api.common.security.challenge.impl.ApiErrorChallengeSender;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/** The configuration for the challenge wrt header based security. */
public class ChallengeConfiguration {

  @Produces
  @ApplicationScoped
  @LookupIfProperty(
      name = "stargate.auth.header-based.custom-challenge-enabled",
      stringValue = "true",
      lookupIfMissing = true)
  ChallengeSender apiErrorChallengeSender(AuthConfig config, ObjectMapper objectMapper) {
    String headerName = config.headerBased().headerName();
    return new ApiErrorChallengeSender(headerName, objectMapper);
  }
}
