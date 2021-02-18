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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import graphql.language.ListType;
import graphql.language.Type;
import io.stargate.auth.AuthenticationService;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreFactory;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.schemafirst.processor.EntityMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.FieldMappingModel;
import io.stargate.graphql.schema.schemafirst.processor.MappingModel;
import io.stargate.graphql.schema.schemafirst.util.TypeHelper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * Base class for fetchers that are generated at runtime for a user's deployed schema (i.e. the
 * GraphQL operations under {@code /namespace/xxx} endpoints).
 */
abstract class DynamicFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final MappingModel mappingModel;

  public DynamicFetcher(
      MappingModel mappingModel,
      AuthenticationService authenticationService,
      AuthorizationService authorizationService,
      DataStoreFactory dataStoreFactory) {
    super(authenticationService, authorizationService, dataStoreFactory);
    this.mappingModel = mappingModel;
  }

  protected Object toCqlValue(Object graphqlValue, Column.ColumnType cqlType, Keyspace keyspace) {
    if (graphqlValue == null) {
      return null;
    }

    if (cqlType.isParameterized()) {
      if (cqlType.rawType() == Column.Type.List) {
        return toCqlCollection(graphqlValue, cqlType, keyspace, ArrayList::new);
      }
      if (cqlType.rawType() == Column.Type.Set) {
        return toCqlCollection(
            graphqlValue, cqlType, keyspace, Sets::newLinkedHashSetWithExpectedSize);
      }
      // Map, tuple
      throw new AssertionError(
          String.format(
              "Unsupported CQL type %s, this mapping should have failed at deployment time",
              cqlType));
    }

    if (cqlType.isUserDefined()) {
      return toCqlUdtValue(graphqlValue, cqlType, keyspace);
    }

    // 'ID' built-in scalar
    if (cqlType == Column.Type.Uuid && graphqlValue instanceof String) {
      return UUID.fromString(((String) graphqlValue));
    }

    // Otherwise it's either:
    // - a built-in scalar that has a natural mapping
    // - one of our custom CQL scalars, and the value has already been coerced
    return graphqlValue;
  }

  private Collection<Object> toCqlCollection(
      Object graphqlValue,
      Column.ColumnType cqlType,
      Keyspace keyspace,
      IntFunction<Collection<Object>> constructor) {
    Column.ColumnType elementType = cqlType.parameters().get(0);
    Collection<?> graphqlCollection = (Collection<?>) graphqlValue;
    return graphqlCollection.stream()
        .map(element -> toCqlValue(element, elementType, keyspace))
        .collect(Collectors.toCollection(() -> constructor.apply(graphqlCollection.size())));
  }

  @SuppressWarnings("unchecked")
  private UdtValue toCqlUdtValue(
      Object graphqlValue, Column.ColumnType cqlType, Keyspace keyspace) {

    String udtName = cqlType.name();
    EntityMappingModel udtModel = mappingModel.getEntities().get(udtName);
    if (udtModel == null) {
      throw new IllegalStateException(
          String.format("UDT '%s' is not mapped to a GraphQL type", udtName));
    }

    // Look up the full definition from the schema. We can't use model.getUdtCqlSchema() because it
    // might contain shallow UDT references.
    UserDefinedType udt = keyspace.userDefinedType(udtName);
    if (udt == null) {
      throw new IllegalStateException(
          String.format(
              "Unknown UDT %s. It looks like it was dropped manually after the deployment.",
              udtName));
    }
    UdtValue udtValue = udt.create();

    Map<String, Object> graphqlObject = (Map<String, Object>) graphqlValue;
    for (FieldMappingModel field : udtModel.getRegularColumns()) {
      if (graphqlObject.containsKey(field.getGraphqlName())) {
        Column.ColumnType fieldCqlType = udt.fieldType(field.getCqlName());
        if (fieldCqlType == null) {
          throw new IllegalStateException(
              String.format(
                  "Unknown field %s in UDT %s. "
                      + "It looks like it was altered manually after the deployment.",
                  field.getCqlName(), udtName));
        }
        Object fieldGraphqlValue = graphqlObject.get(field.getGraphqlName());
        Object fieldCqlValue = toCqlValue(fieldGraphqlValue, fieldCqlType, keyspace);
        udtValue = udtValue.set(field.getCqlName(), fieldCqlValue, fieldCqlType.codec());
      }
    }
    return udtValue;
  }

  protected Object toGraphqlValue(Object cqlValue, Column.ColumnType cqlType, Type<?> graphqlType) {
    if (cqlValue == null) {
      return null;
    }
    if (cqlType.isParameterized()) {
      if (cqlType.rawType() == Column.Type.List) {
        return toGraphqlList(cqlValue, cqlType, graphqlType);
      }
      if (cqlType.rawType() == Column.Type.Set) {
        return toGraphqlList(cqlValue, cqlType, graphqlType);
      }
      // Map, tuple
      throw new AssertionError(
          String.format(
              "Unsupported CQL type %s, this mapping should have failed at deployment time",
              cqlType));
    }

    if (cqlType.isUserDefined()) {
      return toGraphqlUdtValue((UdtValue) cqlValue);
    }

    if (cqlType == Column.Type.Uuid && TypeHelper.isGraphqlId(graphqlType)) {
      return cqlValue.toString();
    }

    return cqlValue;
  }

  private Object toGraphqlList(Object cqlValue, Column.ColumnType cqlType, Type<?> graphqlType) {
    Collection<?> cqlCollection = (Collection<?>) cqlValue;
    assert graphqlType instanceof ListType;
    Type<?> graphqlElementType = ((ListType) graphqlType).getType();
    Column.ColumnType cqlElementType = cqlType.parameters().get(0);
    return cqlCollection.stream()
        .map(e -> toGraphqlValue(e, cqlElementType, graphqlElementType))
        .collect(Collectors.toList());
  }

  private Object toGraphqlUdtValue(UdtValue udtValue) {
    String udtName = udtValue.getType().getName().asInternal();
    EntityMappingModel udtModel = mappingModel.getEntities().get(udtName);
    if (udtModel == null) {
      throw new IllegalStateException(
          String.format("UDT '%s' is not mapped to a GraphQL type", udtName));
    }

    Map<String, Object> result =
        Maps.newLinkedHashMapWithExpectedSize(udtModel.getRegularColumns().size());
    for (FieldMappingModel field : udtModel.getRegularColumns()) {
      Object cqlValue = udtValue.getObject(CqlIdentifier.fromInternal(field.getCqlName()));
      result.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()));
    }
    return result;
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
      DataStore dataStore,
      Keyspace keyspace,
      AuthenticationSubject authenticationSubject)
      throws UnauthorizedException {
    List<BuiltCondition> whereConditions = new ArrayList<>();
    for (int i = 0; i < entity.getPrimaryKey().size(); i++) {
      FieldMappingModel field = entity.getPrimaryKey().get(i);
      int finalI = i;
      String inputName = valueNames.map(l -> l.get(finalI)).orElse(field.getGraphqlName());
      Object graphqlValue = values.get(inputName);
      whereConditions.add(
          BuiltCondition.of(
              field.getCqlName(),
              Predicate.EQ,
              toCqlValue(graphqlValue, field.getCqlType(), keyspace)));
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

    ResultSet resultSet;
    try {
      resultSet =
          authorizationService.authorizedDataRead(
              () -> executeUnchecked(query, dataStore),
              authenticationSubject,
              entity.getKeyspaceName(),
              entity.getCqlName(),
              TypedKeyValue.forSelect((BoundSelect) query),
              SourceAPI.GRAPHQL);
    } catch (Exception e) {
      if (e instanceof UnauthorizedException) {
        throw (UnauthorizedException) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
    if (resultSet.hasNoMoreFetchedRows()) {
      return null;
    }
    Row row = resultSet.one();
    Map<String, Object> result = new HashMap<>();
    for (FieldMappingModel field : entity.getAllColumns()) {
      Object cqlValue = row.getObject(field.getCqlName());
      result.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()));
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
