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

import com.google.common.collect.ImmutableMap;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.FieldCoordinates;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.graphql.schema.CassandraFetcher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

class QueryMappingModel {

  // TODO implement more flexible rules
  // This is a basic implementation that only allows a single-entity SELECT by full primary key.
  // There will probably be significant changes when we support more scenarios: partial primary key
  // (returning multiple entities), index lookups, etc

  private final FieldCoordinates coordinates;
  private final EntityMappingModel entity;
  private final Map<String, FieldMappingModel> inputMapping;

  QueryMappingModel(
      FieldCoordinates coordinates,
      EntityMappingModel entity,
      Map<String, FieldMappingModel> inputMapping) {
    this.coordinates = coordinates;
    this.entity = entity;
    this.inputMapping = inputMapping;
  }

  FieldCoordinates getCoordinates() {
    return coordinates;
  }

  DataFetcher<?> buildDataFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    return new CassandraFetcher<Map<String, Object>>(
        authenticationService, authorizationService, dataStoreFactory) {
      @Override
      protected Map<String, Object> get(
          DataFetchingEnvironment environment,
          DataStore dataStore,
          AuthenticationSubject authenticationSubject)
          throws Exception {

        List<BuiltCondition> whereConditions = new ArrayList<>();
        for (Map.Entry<String, FieldMappingModel> entry : inputMapping.entrySet()) {
          String inputName = entry.getKey();
          FieldMappingModel field = entry.getValue();

          // TODO handle non trivial GraphQL=>CQL conversions (see DataTypeMapping)
          Object graphqlValue = environment.getArgument(inputName);
          Object cqlValue =
              (field.getCqlType() == Column.Type.Uuid)
                  ? UUID.fromString(graphqlValue.toString())
                  : graphqlValue;
          whereConditions.add(BuiltCondition.of(field.getCqlName(), Predicate.EQ, cqlValue));
        }

        AbstractBound<?> query =
            dataStore
                .queryBuilder()
                .select()
                .column(
                    entity.getAllColumns().stream()
                        .map(FieldMappingModel::getCqlName)
                        .toArray(String[]::new))
                .from(entity.getKeyspaceName(), entity.getCqlName())
                .where(whereConditions)
                .build()
                .bind();

        ResultSet resultSet = dataStore.execute(query).get();
        if (resultSet.hasNoMoreFetchedRows()) {
          return null;
        }
        Row row = resultSet.one();
        Map<String, Object> result = new HashMap<>();
        for (FieldMappingModel field : entity.getAllColumns()) {
          // TODO handle non trivial CQL=>GraphQL conversions (see DataTypeMapping)
          Object graphqlValue = row.getObject(field.getCqlName());
          result.put(field.getGraphqlName(), graphqlValue);
        }
        return result;
      }
    };
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
    ImmutableMap.Builder<String, FieldMappingModel> inputMapping = ImmutableMap.builder();
    for (int i = 0; i < inputValues.size(); i++) {
      InputValueDefinition argument = inputValues.get(i);
      FieldMappingModel field = primaryKey.get(i);

      Type<?> argumentType = argument.getType();
      if (argumentType instanceof NonNullType) {
        argumentType = ((NonNullType) argumentType).getType();
      }

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

      inputMapping.put(argument.getName(), field);
    }

    return foundErrors
        ? Optional.empty()
        : Optional.of(new QueryMappingModel(coordinates, entity, inputMapping.build()));
  }
}
