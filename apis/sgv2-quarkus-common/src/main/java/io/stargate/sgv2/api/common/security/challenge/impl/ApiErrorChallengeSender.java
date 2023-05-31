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

package io.stargate.sgv2.api.common.security.challenge.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.api.common.security.challenge.ChallengeSender;
import io.vertx.ext.web.RoutingContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responds with API error on send challenge. */
public class ApiErrorChallengeSender implements ChallengeSender {

  private static final Logger LOG = LoggerFactory.getLogger(ApiErrorChallengeSender.class);

  /** The name of the header to be used for the authentication. */
  private final String headerName;

  /** Object mapper for custom response. */
  private final ObjectMapper objectMapper;

  public ApiErrorChallengeSender(String headerName, ObjectMapper objectMapper) {
    this.headerName = headerName;
    this.objectMapper = objectMapper;
  }

  @Override
  public Uni<Boolean> apply(RoutingContext context, ChallengeData challengeData) {
    int status = challengeData.status;
    context.response().setStatusCode(status);

    // create the response
    String message =
        "Role unauthorized for operation: Missing token, expecting one in the %s header."
            .formatted(headerName);
    ApiError apiError = new ApiError(message, status);
    try {
      // try to serialize
      String response = objectMapper.writeValueAsString(apiError);

      // set content type
      context.response().headers().set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

      // set content length
      context
          .response()
          .headers()
          .set(HttpHeaders.CONTENT_LENGTH, String.valueOf(response.getBytes().length));

      // write and map to true
      return Uni.createFrom()
          .completionStage(context.response().write(response).map(true).toCompletionStage());
    } catch (JsonProcessingException e) {
      LOG.error("Unable to serialize API error instance {} to JSON.", apiError, e);
      return Uni.createFrom().item(true);
    }
  }
}
