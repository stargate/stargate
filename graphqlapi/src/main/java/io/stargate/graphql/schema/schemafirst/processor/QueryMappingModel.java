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
import graphql.schema.FieldCoordinates;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryMappingModel {

  // TODO implement more flexible rules
  // This is a basic implementation that only allows a single-entity SELECT by full primary key.
  // There will probably be significant changes when we support more scenarios: partial primary key
  // (returning multiple entities), index lookups, etc

  private final FieldCoordinates coordinates;
  private final EntityMappingModel entity;
  private final List<String> inputNames;

  QueryMappingModel(
      FieldCoordinates coordinates, EntityMappingModel entity, List<String> inputNames) {
    this.coordinates = coordinates;
    this.entity = entity;
    this.inputNames = inputNames;
  }

  public FieldCoordinates getCoordinates() {
    return coordinates;
  }

  public EntityMappingModel getEntity() {
    return entity;
  }

  public List<String> getInputNames() {
    return inputNames;
  }

  static Optional<QueryMappingModel> build(
      FieldDefinition query,
      String parentTypeName,
      Map<String, EntityMappingModel> entities,
      ProcessingContext context) {

    FieldCoordinates coordinates = FieldCoordinates.coordinates(parentTypeName, query.getName());

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
        : Optional.of(new QueryMappingModel(coordinates, entity, inputNames.build()));
  }
}
