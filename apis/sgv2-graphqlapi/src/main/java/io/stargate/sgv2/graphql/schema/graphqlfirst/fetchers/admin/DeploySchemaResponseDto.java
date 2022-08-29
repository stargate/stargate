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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.admin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.GraphqlErrorHelper;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.sgv2.graphql.schema.graphqlfirst.migration.MigrationQuery;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ProcessingLogType;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ProcessingMessage;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeploySchemaResponseDto {

  private UUID version;
  private List<ProcessingMessage<ProcessingLogType>> logs;
  private List<MigrationQuery> cqlChanges;

  public void setVersion(UUID version) {
    this.version = version;
  }

  public UUID getVersion() {
    return version;
  }

  public void setLogs(List<ProcessingMessage<ProcessingLogType>> logs) {
    this.logs = logs;
  }

  public List<Map<String, Object>> getLogs(DataFetchingEnvironment environment) {
    Stream<ProcessingMessage<ProcessingLogType>> stream = logs.stream();
    ProcessingLogType category = environment.getArgument("category");
    if (category != null) {
      stream = stream.filter(m -> m.getErrorType() == category);
    }
    return stream.map(this::formatMessage).collect(Collectors.toList());
  }

  public void setCqlChanges(List<MigrationQuery> cqlChanges) {
    this.cqlChanges = cqlChanges;
  }

  public List<String> getCqlChanges() {
    return cqlChanges.isEmpty()
        ? ImmutableList.of("No changes, the CQL schema is up to date")
        : cqlChanges.stream().map(MigrationQuery::getDescription).collect(Collectors.toList());
  }

  private Map<String, Object> formatMessage(ProcessingMessage<ProcessingLogType> message) {
    return ImmutableMap.of(
        "message",
        message.getMessage(),
        "category",
        message.getErrorType(),
        "locations",
        message.getLocations().stream()
            .map(GraphqlErrorHelper::location)
            .collect(Collectors.toList()));
  }
}
