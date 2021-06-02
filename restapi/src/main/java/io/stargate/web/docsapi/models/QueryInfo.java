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
package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.swagger.annotations.ApiModelProperty;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableQueryInfo.class)
@JsonDeserialize(as = ImmutableQueryInfo.class)
@Value.Immutable(lazyhash = true)
public interface QueryInfo {

  @ApiModelProperty("CQL query text")
  @JsonProperty("cql")
  String preparedCQL();

  @ApiModelProperty("The number of times this query was executed")
  @JsonProperty("executionCount")
  int execCount();

  @ApiModelProperty("The total number of rows fetched by this query")
  @JsonProperty("rowCount")
  int rowCount();

  static QueryInfo of(String cql, int rowCount) {
    return of(cql, 1, rowCount);
  }

  static QueryInfo of(String cql, int execCount, int rowCount) {
    return ImmutableQueryInfo.builder()
        .execCount(execCount)
        .rowCount(rowCount)
        .preparedCQL(cql)
        .build();
  }

  static QueryInfo combine(QueryInfo i1, QueryInfo i2) {
    String cql = i1.preparedCQL();

    if (!cql.equals(i2.preparedCQL())) {
      throw new IllegalStateException(
          "Unable to combine stats from different CQL queries: "
              + cql
              + " and "
              + i2.preparedCQL());
    }

    return ImmutableQueryInfo.builder()
        .preparedCQL(cql)
        .execCount(i1.execCount() + i2.execCount())
        .rowCount(i1.rowCount() + i2.rowCount())
        .build();
  }
}
