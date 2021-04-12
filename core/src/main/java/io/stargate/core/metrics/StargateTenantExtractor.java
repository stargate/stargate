/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.core.metrics;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StargateTenantExtractor {

  private static final Pattern TENANT_PATTERN =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}");

  private static final String[] HOST_HEADERS = new String[] {"x-forwarded-host", "x-host"};

  /**
   * Returns tenant tag from the headers. If not found returns <code>null</code>.
   *
   * @param headers Header map
   * @return Tag or null
   */
  public static String tenantFromHeaders(Map<String, List<String>> headers) {
    List<String> hostValues = headerValues(headers, HOST_HEADERS);
    for (String host : hostValues) {
      Optional<String> tenant = tenantFromHost(host);
      if (tenant.isPresent()) {
        return tenant.get();
      }
    }
    return null;
  }

  /**
   * Finds header values from the given header map by searching all of the given #header values,
   * with first wins approach.
   *
   * @param headers Header map
   * @param header Header(s) to search for
   * @return Founded value list or empty list
   */
  private static List<String> headerValues(Map<String, List<String>> headers, String... header) {
    if (null != header) {
      for (String search : header) {
        for (Map.Entry<String, List<String>> e : headers.entrySet()) {
          if (search.equalsIgnoreCase(e.getKey())) {
            return e.getValue();
          }
        }
      }
    }
    return Collections.emptyList();
  }

  /**
   * Extracts tenant id from the host.
   *
   * @param host Host
   * @return Optional tenant id.
   */
  private static Optional<String> tenantFromHost(String host) {
    int firstDotIndex = host.indexOf('.');
    if (firstDotIndex >= 36) {
      String tenantWithRegion = host.substring(0, firstDotIndex);
      Matcher matcher = TENANT_PATTERN.matcher(tenantWithRegion);
      if (matcher.find()) {
        return Optional.of(matcher.group());
      }
    }
    return Optional.empty();
  }
}
