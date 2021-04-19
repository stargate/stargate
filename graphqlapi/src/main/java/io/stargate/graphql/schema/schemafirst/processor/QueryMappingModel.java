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

import com.google.common.collect.ImmutableList;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.graphql.schema.schemafirst.fetchers.dynamic.QueryFetcher;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryMappingModel extends OperationMappingModel {

  // TODO implement more flexible rules
  // This is a basic implementation that only allows a single-entity SELECT by full primary key.
  // There will probably be significant changes when we support more scenarios: partial primary key
  // (returning multiple entities), index lookups, etc

  private final EntityMappingModel entity;
  private final List<String> inputNames;

  private QueryMappingModel(
      String parentTypeName,
      FieldDefinition field,
      EntityMappingModel entity,
      List<String> inputNames) {
    super(parentTypeName, field);
    this.entity = entity;
    this.inputNames = inputNames;
  }

  public EntityMappingModel getEntity() {
    return entity;
  }

  public List<String> getInputNames() {
    return inputNames;
  }

  @Override
  public DataFetcher<?> getDataFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    return new QueryFetcher(this, authenticationService, authorizationService, dataStoreFactory);
  }

  static Optional<QueryMappingModel> build(
      FieldDefinition query,
      String parentTypeName,
      Map<String, EntityMappingModel> entities,
      ProcessingContext context) {

    Type<?> returnType = query.getType();
    String entityName = (returnType instanceof TypeName) ? ((TypeName) returnType).getName() : null;
    if (entityName == null || !entities.containsKey(entityName)) {
      context.addError(
          query.getSourceLocation(),
          ProcessingMessageType.InvalidMapping,
          "Expected the query type to be an object that maps to an entity");
      return Optional.empty();
    }

    EntityMappingModel entity = entities.get(entityName);

    List<InputValueDefinition> inputValues = query.getInputValueDefinitions();
    List<FieldMappingModel> primaryKey = entity.getPrimaryKey();
    if (inputValues.size() != primaryKey.size()) {
      context.addError(
          query.getSourceLocation(),
          ProcessingMessageType.InvalidMapping,
          "Expected number of query arguments (%d) "
              + "to match number of partition key + clustering column fields on the entity (%d)",
          inputValues.size(),
          primaryKey.size());
      return Optional.empty();
    }

    boolean foundErrors = false;
    ImmutableList.Builder<String> inputNames = ImmutableList.builder();
    for (int i = 0; i < inputValues.size(); i++) {
      InputValueDefinition argument = inputValues.get(i);
      FieldMappingModel field = primaryKey.get(i);

      Type<?> argumentType = argument.getType();
      if (!argumentType.isEqualTo(field.getGraphqlType())) {
        context.addError(
            argument.getSourceLocation(),
            ProcessingMessageType.InvalidMapping,
            "Expected argument %s to have the same type as %s.%s",
            argument.getName(),
            entity.getGraphqlName(),
            field.getGraphqlName());
        foundErrors = true;
      }

      inputNames.add(argument.getName());
    }

    return foundErrors
        ? Optional.empty()
        : Optional.of(new QueryMappingModel(parentTypeName, query, entity, inputNames.build()));
  }
}
