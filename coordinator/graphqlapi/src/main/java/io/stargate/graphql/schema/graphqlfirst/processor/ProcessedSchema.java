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
package io.stargate.graphql.schema.graphqlfirst.processor;

import graphql.GraphQL;
import java.util.List;

public class ProcessedSchema {

  private final MappingModel mappingModel;
  private final GraphQL graphql;
  private final List<ProcessingMessage<ProcessingLogType>> logs;

  public ProcessedSchema(
      MappingModel mappingModel, GraphQL graphql, List<ProcessingMessage<ProcessingLogType>> logs) {
    this.mappingModel = mappingModel;
    this.graphql = graphql;
    this.logs = logs;
  }

  public MappingModel getMappingModel() {
    return mappingModel;
  }

  public GraphQL getGraphql() {
    return graphql;
  }

  public List<ProcessingMessage<ProcessingLogType>> getLogs() {
    return logs;
  }
}
