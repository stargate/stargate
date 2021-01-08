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
package io.stargate.graphql.schema.schemafirst.processor;

import graphql.GraphQL;
import io.stargate.db.datastore.DataStore;
import java.util.List;

public class ProcessedSchema {

  private final GraphQL graphql;
  private final MappingModel mappingModel;
  private final List<ProcessingMessage> messages;

  public ProcessedSchema(
      GraphQL graphql, MappingModel mappingModel, List<ProcessingMessage> messages) {
    this.graphql = graphql;
    this.mappingModel = mappingModel;
    this.messages = messages;
  }

  public GraphQL getGraphql() {
    return graphql;
  }

  public List<ProcessingMessage> getMessages() {
    return messages;
  }

  public void dropAndRecreateSchema(DataStore dataStore) throws Exception {
    for (EntityMappingModel entity : mappingModel.getEntities().values()) {
      dataStore.execute(entity.buildDropQuery(dataStore.queryBuilder())).get();
      dataStore.execute(entity.buildCreateQuery(dataStore.queryBuilder())).get();
    }
  }
}
