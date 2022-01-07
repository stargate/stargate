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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.restsvc.driver;

import com.datastax.oss.driver.api.core.CqlSession;
import java.util.UUID;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Creates a driver session wrapper for the current HTTP request, if it includes a token and/or
 * tenant ID.
 *
 * @see ScopedCqlSessionFactory
 */
public class CreateScopedCqlSessionFilter implements ContainerRequestFilter {

  public static final String SCOPED_SESSION_KEY =
      "io.stargate.sgv2.restsvc.resources.ScopedSession";

  private static final String HOST_HEADER_NAME = "Host";
  private static final String HOST_HEADER_NAME_LOWER = "host";
  private static final String TENANT_ID_HEADER_NAME = "tenant_id";

  private final CqlSession mainSession;

  /** @param mainSession the "real" driver instance that will get wrapped. */
  public CreateScopedCqlSessionFilter(CqlSession mainSession) {
    this.mainSession = mainSession;
  }

  @Override
  public void filter(ContainerRequestContext context) {
    String token = context.getHeaderString("X-Cassandra-Token");
    String tenantId = extractTenantId(context.getHeaders());
    if (token != null || tenantId != null) {
      context.setProperty(SCOPED_SESSION_KEY, new ScopedCqlSession(mainSession, token, tenantId));
    }
  }

  private String extractTenantId(MultivaluedMap<String, String> headers) {
    String fromTenantIdHeader = headers.getFirst(TENANT_ID_HEADER_NAME);
    if (fromTenantIdHeader != null) {
      return fromTenantIdHeader;
    } else {
      String hostheader = headers.getFirst(HOST_HEADER_NAME);
      if (hostheader == null) {
        hostheader = headers.getFirst(HOST_HEADER_NAME_LOWER);
      }
      return extractTenantIdFromHostHeader(hostheader);
    }
  }

  private static String extractTenantIdFromHostHeader(String hostHeader) {
    if (hostHeader == null || hostHeader.length() < 36) {
      return null;
    }
    String tenantIdCandidate = hostHeader.substring(0, 36);
    // create UUID from tenant id to assure that it is in a proper format
    try {
      return UUID.fromString(tenantIdCandidate).toString();
    } catch (IllegalArgumentException exception) {
      return null;
    }
  }
}
