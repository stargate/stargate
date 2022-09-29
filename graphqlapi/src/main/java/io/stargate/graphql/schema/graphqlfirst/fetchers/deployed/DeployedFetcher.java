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
package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import graphql.Scalars;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.schema.GraphQLScalarType;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.UserDefinedType;
import io.stargate.graphql.schema.CassandraFetcher;
import io.stargate.graphql.schema.graphqlfirst.processor.ConditionModel;
import io.stargate.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.graphql.schema.scalars.CqlScalar;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Base class for fetchers that handle the queries from a user's deployed schema. */
abstract class DeployedFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final MappingModel mappingModel;

  public DeployedFetcher(MappingModel mappingModel) {
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

    assert cqlType instanceof Column.Type;
    Column.Type cqlScalarType = (Column.Type) cqlType;
    // Most of the time the GraphQL runtime has already coerced the value. But if we come from a
    // Federation `_entities` query, this is not possible because the representations are not
    // strongly typed, and the type of each field can't be guessed. We have to do it manually:
    Class<?> expectedClass;
    GraphQLScalarType graphqlScalar;
    switch (cqlScalarType) {
        // Built-in GraphQL scalars:
      case Int:
        expectedClass = Integer.class;
        graphqlScalar = Scalars.GraphQLInt;
        break;
      case Boolean:
        expectedClass = Boolean.class;
        graphqlScalar = Scalars.GraphQLBoolean;
        break;
      case Double:
        expectedClass = Double.class;
        graphqlScalar = Scalars.GraphQLFloat;
        break;
      case Text:
        expectedClass = String.class;
        graphqlScalar = Scalars.GraphQLString;
        break;
      default:
        // Our custom CQL scalars:
        CqlScalar cqlScalar =
            CqlScalar.fromCqlType(cqlScalarType)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported type " + cqlType));
        expectedClass = cqlScalar.getCqlValueClass();
        graphqlScalar = cqlScalar.getGraphqlType();
        break;
    }
    return expectedClass.isInstance(graphqlValue)
        ? graphqlValue
        : graphqlScalar.getCoercing().parseValue(graphqlValue);
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
    EntityModel udtModel = mappingModel.getEntities().get(udtName);
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
    for (FieldModel field : udtModel.getRegularColumns()) {
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
    EntityModel udtModel = mappingModel.getEntities().get(udtName);
    if (udtModel == null) {
      throw new IllegalStateException(
          String.format("UDT '%s' is not mapped to a GraphQL type", udtName));
    }

    Map<String, Object> result =
        Maps.newLinkedHashMapWithExpectedSize(udtModel.getRegularColumns().size());
    for (FieldModel field : udtModel.getRegularColumns()) {
      Object cqlValue = udtValue.getObject(CqlIdentifier.fromInternal(field.getCqlName()));
      result.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()));
    }
    return result;
  }

  /** Queries one or more instances of an entity for the given conditions. */
  protected ResultSet query(
      EntityModel entity,
      List<BuiltCondition> whereConditions,
      Optional<Integer> limit,
      Parameters parameters,
      StargateGraphqlContext context)
      throws UnauthorizedException {

    AbstractBound<?> query =
        context
            .getDataStore()
            .queryBuilder()
            .select()
            .column(
                entity.getAllColumns().stream().map(FieldModel::getCqlName).toArray(String[]::new))
            .from(entity.getKeyspaceName(), entity.getCqlName())
            .where(whereConditions)
            .limit(limit.orElse(null))
            .build()
            .bind();

    try {
      return context
          .getAuthorizationService()
          .authorizedDataRead(
              () -> executeUnchecked(query, parameters, context),
              context.getSubject(),
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
  }

  protected Map<String, Object> toSingleEntity(ResultSet resultSet, EntityModel entity) {
    return resultSet.hasNoMoreFetchedRows() ? null : toEntity(resultSet.one(), entity);
  }

  protected List<Map<String, Object>> toEntities(ResultSet resultSet, EntityModel entity) {
    return resultSet.currentPageRows().stream()
        .map(row -> toEntity(row, entity))
        .collect(Collectors.toList());
  }

  private Map<String, Object> toEntity(Row row, EntityModel entity) {
    Map<String, Object> singleResult = new HashMap<>();
    for (FieldModel field : entity.getAllColumns()) {
      Object cqlValue = row.getObject(field.getCqlName());
      singleResult.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()));
    }
    return singleResult;
  }

  /**
   * Copies a CQL row to a map representing the given entity. Only the columns that correspond to
   * the entity's fields are considered. If some fields were already present in the map, they are
   * overridden.
   */
  protected void copyRowToEntity(Row row, Map<String, Object> entityData, EntityModel entity) {
    for (FieldModel field : entity.getAllColumns()) {
      if (row.columns().stream().noneMatch(c -> c.name().equals(field.getCqlName()))) {
        continue;
      }
      Object cqlValue = row.getObject(field.getCqlName());
      entityData.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()));
    }
  }

  protected ResultSet executeUnchecked(
      AbstractBound<?> query, Parameters parameters, StargateGraphqlContext context) {
    try {
      return context.getDataStore().execute(query, __ -> parameters).get();
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

  /**
   * Given the WHERE conditions that were inferred from an operation signature, find out which
   * arguments are actually present in an runtime invocation, and bind the corresponding conditions.
   *
   * <p>Example:
   *
   * <pre>
   * // Query definition
   * type Query { readings(sensorId: ID!, hour: Int, minute: Int): [SensorReading] }
   *
   * // Inferred conditions:
   * sensorId = ? AND hour = ? AND minute = ?
   *
   * // GraphQL call:
   * { readings(sensorId: "xyz", hour: 12) { value } }
   *
   * // Actual conditions:
   * sensorId = 'xyz' AND hour = 12
   * </pre>
   *
   * @param hasArgument how to know if an argument is present (this is abstracted because in some
   *     cases the values are not directly arguments, but instead inner fields of an object
   *     argument).
   * @param getArgument how to get the value of an argument (same).
   * @param validator a validation function that will be applied to the actual conditions, to check
   *     that they form a valid where clause.
   */
  protected List<BuiltCondition> bindWhere(
      List<ConditionModel> conditions,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument,
      Function<List<ConditionModel>, Optional<String>> validator,
      Keyspace keyspace) {

    List<BuiltCondition> result = new ArrayList<>();
    List<ConditionModel> activeConditions =
        bind(conditions, hasArgument, getArgument, keyspace, result);

    validator
        .apply(activeConditions)
        .ifPresent(
            message -> {
              throw new IllegalArgumentException("Invalid arguments: " + message);
            });
    return result;
  }

  protected List<BuiltCondition> bindIf(
      List<ConditionModel> conditions,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument,
      Keyspace keyspace) {

    List<BuiltCondition> result = new ArrayList<>();
    bind(conditions, hasArgument, getArgument, keyspace, result);
    return result;
  }

  private <T extends ConditionModel> List<T> bind(
      List<T> conditions,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument,
      Keyspace keyspace,
      List<BuiltCondition> result) {
    List<T> activeConditions = new ArrayList<>();
    for (T condition : conditions) {
      FieldModel field = condition.getField();
      if (hasArgument.test(condition.getArgumentName())) {
        activeConditions.add(condition);
        Object graphqlValue = getArgument.apply(condition.getArgumentName());
        Column.ColumnType cqlType;
        switch (condition.getPredicate()) {
          case IN:
            cqlType = Column.Type.List.of(field.getCqlType());
            break;
          case CONTAINS:
            cqlType = field.getCqlType().parameters().get(0);
            break;
          default:
            cqlType = field.getCqlType();
            break;
        }
        Object cqlValue = toCqlValue(graphqlValue, cqlType, keyspace);
        result.add(condition.build(cqlValue));
      }
    }
    return activeConditions;
  }
}
