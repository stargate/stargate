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
package io.stargate.sgv2.docsapi.api.v2.model.dto;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

public record QueryInfo(
    @Schema(description = "CQL query text") String cql,
    @Schema(description = "The number of times this query was executed") int executionCount,
    @Schema(
            description =
                "The total number of rows fetched or modified by this query. "
                    + "Note that this is underestimated when the query involves deletions: there is no "
                    + "efficient way (without a read-before-write) to count how many rows a CQL DELETE will "
                    + "remove, so it's always counted as 1.")
        int rowCount) {

  public static QueryInfo of(String cql, int rowCount) {
    return of(cql, 1, rowCount);
  }

  public static QueryInfo of(String cql, int execCount, int rowCount) {
    return new QueryInfo(cql, execCount, rowCount);
  }

  public static QueryInfo combine(QueryInfo i1, QueryInfo i2) {
    String cql = i1.cql();

    if (!cql.equals(i2.cql())) {
      throw new IllegalStateException(
          "Unable to combine stats from different CQL queries: " + cql + " and " + i2.cql());
    }

    return new QueryInfo(
        cql, i1.executionCount() + i2.executionCount(), i1.rowCount() + i2.rowCount());
  }
}
