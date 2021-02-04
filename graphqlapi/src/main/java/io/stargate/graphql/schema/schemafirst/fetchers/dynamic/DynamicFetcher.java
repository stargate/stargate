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
package io.stargate.graphql.schema.schemafirst.fetchers.dynamic;

import io.stargate.auth.AuthenticationService;
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
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.FieldMappingModel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Base class for fetchers that are generated at runtime for a user's deployed schema (i.e. the
 * GraphQL operations under {@code /namespace/xxx} endpoints).
 */
abstract class DynamicFetcher<ResultT> extends CassandraFetcher<ResultT> {

  public DynamicFetcher(
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
  }

  protected Object toCqlValue(Object graphqlValue, FieldMappingModel field) {
    // TODO handle non trivial GraphQL=>CQL conversions (see DataTypeMapping)
    return (field.getCqlType() == Column.Type.Uuid)
        ? UUID.fromString(graphqlValue.toString())
        : graphqlValue;
  }

  protected Object toGraphqlValue(Object cqlValue, FieldMappingModel field) {
    // TODO handle non trivial CQL=>GraphQL conversions (see DataTypeMapping)
    return cqlValue;
  }

  /**
   * Builds and executes a query to fetch a single instance of the given entity by its PK.
   *
   * @param valueNames if present, the keys for each PK component in {@code values}; otherwise, the
   *     name of the field will be used.
   */
  protected Map<String, Object> querySingleEntity(
      EntityMappingModel entity,
      Map<String, Object> values,
      Optional<List<String>> valueNames,
      DataStore dataStore) {
    List<BuiltCondition> whereConditions = new ArrayList<>();
    for (int i = 0; i < entity.getPrimaryKey().size(); i++) {
      FieldMappingModel field = entity.getPrimaryKey().get(i);
      int finalI = i;
      String inputName = valueNames.map(l -> l.get(finalI)).orElse(field.getGraphqlName());
      Object graphqlValue = values.get(inputName);
      whereConditions.add(
          BuiltCondition.of(field.getCqlName(), Predicate.EQ, toCqlValue(graphqlValue, field)));
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

    ResultSet resultSet = executeUnchecked(query, dataStore);
    if (resultSet.hasNoMoreFetchedRows()) {
      return null;
    }
    Row row = resultSet.one();
    Map<String, Object> result = new HashMap<>();
    for (FieldMappingModel field : entity.getAllColumns()) {
      Object cqlValue = row.getObject(field.getCqlName());
      result.put(field.getGraphqlName(), toGraphqlValue(cqlValue, field));
    }
    return result;
  }

  protected ResultSet executeUnchecked(AbstractBound<?> query, DataStore dataStore) {
    try {
      return dataStore.execute(query).get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw ((Error) cause);
      } else if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }
}
