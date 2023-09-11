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

package io.stargate.sgv2.api.common.logging;

import static io.stargate.sgv2.api.common.config.constants.LoggingConstants.*;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.LoggingConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Set;
import org.jboss.resteasy.reactive.server.ServerResponseFilter;
import org.slf4j.Logger;

/** The filter for logging requests when required for debugging purposes. */
@ApplicationScoped
public class LoggingFilter {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(LoggingFilter.class);

  /** The request info bean. */
  private final StargateRequestInfo requestInfo;
  /** The configuration for logging. */
  private final LoggingConfig loggingConfig;

  /** Default constructor. */
  @Inject
  public LoggingFilter(StargateRequestInfo requestInfo, LoggingConfig loggingConfig) {
    this.requestInfo = requestInfo;
    this.loggingConfig = loggingConfig;
  }

  /**
   * Filter that this bean produces.
   *
   * @param requestContext {@link ContainerRequestContext}
   * @param responseContext {@link ContainerResponseContext}
   * @see https://quarkus.io/guides/resteasy-reactive#request-or-response-filters
   */
  @ServerResponseFilter
  public void logRequestIfRequired(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    if (isLoggingAllowed(requestContext, responseContext)) {
      logger.debug(
          buildRequestInfo(
              requestContext, responseContext, loggingConfig.requestBodyLoggingEnabled()));
    }
  }

  private String buildRequestInfo(
      ContainerRequestContext requestContext,
      ContainerResponseContext responseContext,
      boolean logRequestBody) {
    String requestBody = logRequestBody ? getRequestBody(requestContext) : "";
    return String.format(
        "REQUEST INFO :: %s %s %s %s %s",
        requestInfo.getTenantId().orElse("tenant-unknown"),
        responseContext.getStatus(),
        requestContext.getMethod(),
        requestContext.getUriInfo().getPath(),
        requestBody);
  }

  private String getRequestBody(ContainerRequestContext requestContext) {
    try {
      InputStream inputStream = requestContext.getEntityStream();
      if (inputStream == null || !inputStream.markSupported()) {
        return "";
      }
      inputStream.reset();
      return CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
    } catch (IOException e) {
      logger.error("Exception occurred while trying to log request info", e);
      return "";
    }
  }

  private boolean isLoggingAllowed(
      ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    if (!loggingConfig.enabled()) {
      return false;
    }
    Set<String> allowedTenants =
        loggingConfig.enabledTenants().orElse(Collections.singleton(ALL_TENANTS));
    if (!allowedTenants.contains(ALL_TENANTS)
        && !allowedTenants.contains(requestInfo.getTenantId().orElse(""))) {
      return false;
    }
    Set<String> allowedPaths =
        loggingConfig.enabledPaths().orElse(Collections.singleton(ALL_PATHS));
    if (!allowedPaths.contains(ALL_PATHS)
        && !allowedPaths.contains(requestContext.getUriInfo().getPath())) {
      return false;
    }
    Set<String> allowedPathPrefixes =
        loggingConfig.enabledPathPrefixes().orElse(Collections.singleton(ALL_PATH_PREFIXES));
    if (!allowedPathPrefixes.contains(ALL_PATH_PREFIXES)) {
      boolean found = false;
      for (String prefix : allowedPathPrefixes) {
        if (requestContext.getUriInfo().getPath().startsWith(prefix)) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;
      }
    }
    Set<String> allowedErrorCodes =
        loggingConfig.enabledErrorCodes().orElse(Collections.singleton(ALL_ERROR_CODES));
    if (!allowedErrorCodes.contains(ALL_ERROR_CODES)
        && !allowedErrorCodes.contains(String.valueOf(responseContext.getStatus()))) {
      return false;
    }
    Set<String> allowedMethods =
        loggingConfig.enabledMethods().orElse(Collections.singleton(ALL_METHODS));
    if (!allowedMethods.contains(ALL_METHODS)
        && !allowedMethods.contains(requestContext.getMethod())) {
      return false;
    }
    return true;
  }
}
