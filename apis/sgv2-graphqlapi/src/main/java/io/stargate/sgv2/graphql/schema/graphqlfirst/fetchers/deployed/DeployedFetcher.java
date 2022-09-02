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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import com.google.common.collect.Maps;
import graphql.Scalars;
import graphql.language.ListType;
import graphql.language.Type;
import graphql.schema.GraphQLScalarType;
import io.stargate.bridge.grpc.CqlDuration;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec;
import io.stargate.bridge.proto.QueryOuterClass.TypeSpec.Udt;
import io.stargate.bridge.proto.QueryOuterClass.UdtValue;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.api.common.grpc.proto.Rows;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.ConditionModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.EntityModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.FieldModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.processor.MappingModel;
import io.stargate.sgv2.graphql.schema.graphqlfirst.util.TypeHelper;
import io.stargate.sgv2.graphql.schema.scalars.CqlScalar;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Base class for fetchers that handle the queries from a user's deployed schema. */
abstract class DeployedFetcher<ResultT> extends CassandraFetcher<ResultT> {

  protected final MappingModel mappingModel;
  protected final CqlKeyspaceDescribe keyspace;

  public DeployedFetcher(MappingModel mappingModel, CqlKeyspaceDescribe keyspace) {
    this.mappingModel = mappingModel;
    this.keyspace = keyspace;
  }

  protected Value toCqlValue(Object graphqlValue, TypeSpec cqlType, CqlKeyspaceDescribe keyspace) {
    if (graphqlValue == null) {
      return Values.NULL;
    }

    switch (cqlType.getSpecCase()) {
      case LIST:
        return toCqlCollection(graphqlValue, cqlType.getList().getElement(), keyspace);
      case SET:
        return toCqlCollection(graphqlValue, cqlType.getSet().getElement(), keyspace);
      case UDT:
        return toCqlUdtValue(graphqlValue, cqlType.getUdt(), keyspace);
      case BASIC:
        TypeSpec.Basic basic = cqlType.getBasic();
        switch (basic) {
          case ASCII:
          case VARCHAR:
            return Values.of(toCqlLiteral(graphqlValue, String.class, Scalars.GraphQLString));
          case INET:
            return Values.of(
                toCqlLiteral(graphqlValue, InetAddress.class, CqlScalar.INET.getGraphqlType()));
          case BIGINT:
          case COUNTER:
            return Values.of(
                toCqlLiteral(graphqlValue, Long.class, CqlScalar.BIGINT.getGraphqlType()));
          case BLOB:
            return Values.of(
                toCqlLiteral(graphqlValue, ByteBuffer.class, CqlScalar.BLOB.getGraphqlType()));
          case BOOLEAN:
            return Values.of(toCqlLiteral(graphqlValue, Boolean.class, Scalars.GraphQLBoolean));
          case DECIMAL:
            return Values.of(
                toCqlLiteral(graphqlValue, BigDecimal.class, CqlScalar.DECIMAL.getGraphqlType()));
          case DOUBLE:
            return Values.of(toCqlLiteral(graphqlValue, Double.class, Scalars.GraphQLFloat));
          case FLOAT:
            return Values.of(
                toCqlLiteral(graphqlValue, Float.class, CqlScalar.FLOAT.getGraphqlType()));
          case INT:
            return Values.of(toCqlLiteral(graphqlValue, Integer.class, Scalars.GraphQLInt));
          case TIMESTAMP:
            return Values.of(
                toCqlLiteral(graphqlValue, Instant.class, CqlScalar.TIMESTAMP.getGraphqlType())
                    .toEpochMilli());
          case UUID:
            return Values.of(
                toCqlLiteral(graphqlValue, UUID.class, CqlScalar.UUID.getGraphqlType()));
          case TIMEUUID:
            return Values.of(
                toCqlLiteral(graphqlValue, UUID.class, CqlScalar.TIMEUUID.getGraphqlType()));
          case VARINT:
            return Values.of(
                toCqlLiteral(graphqlValue, BigInteger.class, CqlScalar.VARINT.getGraphqlType()));
          case DATE:
            return Values.of(
                toCqlLiteral(graphqlValue, LocalDate.class, CqlScalar.DATE.getGraphqlType()));
          case TIME:
            return Values.of(
                toCqlLiteral(graphqlValue, LocalTime.class, CqlScalar.TIME.getGraphqlType()));
          case SMALLINT:
            return Values.of(
                toCqlLiteral(graphqlValue, Short.class, CqlScalar.SMALLINT.getGraphqlType()));
          case TINYINT:
            return Values.of(
                toCqlLiteral(graphqlValue, Byte.class, CqlScalar.TINYINT.getGraphqlType()));
          case DURATION:
            return Values.of(
                toCqlLiteral(graphqlValue, CqlDuration.class, CqlScalar.DURATION.getGraphqlType()));
          default:
            throw new AssertionError("Unsupported primitive type " + cqlType);
        }
      default:
        // In particular, maps and tuples are not supported
        throw new AssertionError(
            String.format(
                "Unsupported CQL type %s, this mapping should have failed at deployment time",
                cqlType));
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T toCqlLiteral(Object value, Class<T> targetClass, GraphQLScalarType scalar) {
    // Most of the time the GraphQL runtime has already coerced the value:
    if (targetClass.isInstance(value)) {
      return targetClass.cast(value);
    }

    // But if we come from a Federation `_entities` query, the representations are loosely typed,
    // and the type of each field can't be guessed, so we have to coerce manually:
    return (T) scalar.getCoercing().parseValue(value);
  }

  private Value toCqlCollection(
      Object graphqlValue, TypeSpec cqlElementType, CqlKeyspaceDescribe keyspace) {
    Collection<?> graphQLCollection = (Collection<?>) graphqlValue;
    Value[] cqlCollection = new Value[graphQLCollection.size()];
    int i = 0;
    for (Object element : graphQLCollection) {
      cqlCollection[i] = toCqlValue(element, cqlElementType, keyspace);
      i++;
    }
    return Values.of(cqlCollection);
  }

  private Value toCqlUdtValue(Object graphqlValue, Udt cqlType, CqlKeyspaceDescribe keyspace) {

    String udtName = cqlType.getName();
    EntityModel udtModel = mappingModel.getEntities().get(udtName);
    if (udtModel == null) {
      throw new IllegalStateException(
          String.format("UDT '%s' is not mapped to a GraphQL type", udtName));
    }

    // Look up the full definition from the schema. We can't use model.getUdtCqlSchema() because it
    // might contain shallow UDT references.
    Udt udt =
        keyspace.getTypesList().stream()
            .filter(t -> udtName.equals(t.getName()))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "Unknown UDT %s. It looks like it was dropped manually after the deployment.",
                            udtName)));

    UdtValue.Builder udtValue = UdtValue.newBuilder();
    @SuppressWarnings("unchecked")
    Map<String, Object> graphqlObject = (Map<String, Object>) graphqlValue;
    for (FieldModel field : udtModel.getRegularColumns()) {
      if (graphqlObject.containsKey(field.getGraphqlName())) {
        TypeSpec fieldCqlType = udt.getFieldsMap().get(field.getCqlName());
        if (fieldCqlType == null) {
          throw new IllegalStateException(
              String.format(
                  "Unknown field %s in UDT %s. "
                      + "It looks like it was altered manually after the deployment.",
                  field.getCqlName(), udtName));
        }
        Object fieldGraphqlValue = graphqlObject.get(field.getGraphqlName());
        Value fieldCqlValue = toCqlValue(fieldGraphqlValue, fieldCqlType, keyspace);
        udtValue.putFields(field.getCqlName(), fieldCqlValue);
      }
    }
    return Value.newBuilder().setUdt(udtValue).build();
  }

  protected Object toGraphqlValue(Value cqlValue, TypeSpec cqlType, Type<?> graphqlType) {
    if (cqlValue.equals(Values.NULL)) {
      return null;
    }
    switch (cqlType.getSpecCase()) {
      case LIST:
        return toGraphqlList(cqlValue.getCollection(), cqlType.getList().getElement(), graphqlType);
      case SET:
        return toGraphqlList(cqlValue.getCollection(), cqlType.getSet().getElement(), graphqlType);
      case UDT:
        return toGraphqlUdtValue(cqlValue.getUdt(), cqlType);
      case BASIC:
        switch (cqlType.getBasic()) {
          case ASCII:
          case VARCHAR:
            return Values.string(cqlValue);
          case INET:
            return Values.inet(cqlValue);
          case BIGINT:
          case COUNTER:
            return Values.bigint(cqlValue);
          case BLOB:
            return Values.byteBuffer(cqlValue);
          case BOOLEAN:
            return Values.bool(cqlValue);
          case DECIMAL:
            return Values.decimal(cqlValue);
          case DOUBLE:
            return Values.double_(cqlValue);
          case FLOAT:
            return Values.float_(cqlValue);
          case INT:
            return Values.int_(cqlValue);
          case TIMESTAMP:
            return Instant.ofEpochMilli(Values.bigint(cqlValue));
          case UUID:
            UUID uuid = Values.uuid(cqlValue);
            return TypeHelper.isGraphqlId(graphqlType) ? uuid.toString() : uuid;
          case TIMEUUID:
            return Values.uuid(cqlValue);
          case VARINT:
            return Values.varint(cqlValue);
          case DATE:
            return Values.date(cqlValue);
          case TIME:
            return Values.time(cqlValue);
          case SMALLINT:
            return Values.smallint(cqlValue);
          case TINYINT:
            return Values.tinyint(cqlValue);
          case DURATION:
            return Values.duration(cqlValue);
          default:
            throw new AssertionError("Unhandled basic type " + cqlType.getBasic());
        }
      default:
        throw new AssertionError(
            String.format(
                "Unsupported CQL type %s, this mapping should have failed at deployment time",
                cqlType));
    }
  }

  private Object toGraphqlList(
      QueryOuterClass.Collection cqlValues, TypeSpec cqlElementType, Type<?> graphqlType) {
    assert graphqlType instanceof ListType;
    Type<?> graphqlElementType = ((ListType) graphqlType).getType();
    return cqlValues.getElementsList().stream()
        .map(e -> toGraphqlValue(e, cqlElementType, graphqlElementType))
        .collect(Collectors.toList());
  }

  private Object toGraphqlUdtValue(UdtValue cqlValue, TypeSpec cqlType) {
    String udtName = cqlType.getUdt().getName();
    EntityModel udtModel = mappingModel.getEntities().get(udtName);
    if (udtModel == null) {
      throw new IllegalStateException(
          String.format("UDT '%s' is not mapped to a GraphQL type", udtName));
    }

    Map<String, Object> result =
        Maps.newLinkedHashMapWithExpectedSize(udtModel.getRegularColumns().size());
    for (FieldModel field : udtModel.getRegularColumns()) {
      Value cqlFieldValue = cqlValue.getFieldsMap().get(field.getCqlName());
      result.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlFieldValue, field.getCqlType(), field.getGraphqlType()));
    }
    return result;
  }

  /** Queries one or more instances of an entity for the given conditions. */
  protected ResultSet query(
      EntityModel entity,
      List<BuiltCondition> whereConditions,
      Optional<Integer> limit,
      QueryParameters parameters,
      StargateGraphqlContext context) {

    Query query =
        new QueryBuilder()
            .select()
            .column(
                entity.getAllColumns().stream().map(FieldModel::getCqlName).toArray(String[]::new))
            .from(entity.getKeyspaceName(), entity.getCqlName())
            .where(whereConditions)
            .limit(limit.orElse(null))
            .parameters(parameters)
            .build();

    return context.getBridge().executeQuery(query).getResultSet();
  }

  protected Map<String, Object> toSingleEntity(ResultSet resultSet, EntityModel entity) {
    return resultSet.getRowsCount() == 0
        ? null
        : toEntity(resultSet.getRows(0), resultSet.getColumnsList(), entity);
  }

  protected List<Map<String, Object>> toEntities(ResultSet resultSet, EntityModel entity) {
    List<ColumnSpec> columns = resultSet.getColumnsList();
    return resultSet.getRowsList().stream()
        .map(row -> toEntity(row, columns, entity))
        .collect(Collectors.toList());
  }

  private Map<String, Object> toEntity(Row row, List<ColumnSpec> columns, EntityModel entity) {
    Map<String, Object> singleResult = new HashMap<>();
    for (FieldModel field : entity.getAllColumns()) {
      Value cqlValue = Rows.getValue(row, field.getCqlName(), columns);
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
  protected void copyRowToEntity(
      Row row, List<ColumnSpec> columns, Map<String, Object> entityData, EntityModel entity) {
    for (FieldModel field : entity.getAllColumns()) {
      if (columns.stream().noneMatch(c -> c.getName().equals(field.getCqlName()))) {
        continue;
      }
      Value cqlValue = Rows.getValue(row, field.getCqlName(), columns);
      entityData.put(
          field.getGraphqlName(),
          toGraphqlValue(cqlValue, field.getCqlType(), field.getGraphqlType()));
    }
  }

  /**
   * Given the WHERE conditions that were inferred from an operation signature, find out which
   * arguments are actually present in a runtime invocation, and bind the corresponding conditions.
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
      CqlKeyspaceDescribe keyspace) {

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
      CqlKeyspaceDescribe keyspace) {

    List<BuiltCondition> result = new ArrayList<>();
    bind(conditions, hasArgument, getArgument, keyspace, result);
    return result;
  }

  private <T extends ConditionModel> List<T> bind(
      List<T> conditions,
      Predicate<String> hasArgument,
      Function<String, Object> getArgument,
      CqlKeyspaceDescribe keyspace,
      List<BuiltCondition> result) {
    List<T> activeConditions = new ArrayList<>();
    for (T condition : conditions) {
      FieldModel field = condition.getField();
      if (hasArgument.test(condition.getArgumentName())) {
        activeConditions.add(condition);
        Object graphqlValue = getArgument.apply(condition.getArgumentName());
        TypeSpec cqlType;
        switch (condition.getPredicate()) {
          case IN:
            cqlType = TypeSpecs.list(field.getCqlType());
            break;
          case CONTAINS:
            switch (field.getCqlType().getSpecCase()) {
              case LIST:
                cqlType = field.getCqlType().getList().getElement();
                break;
              case SET:
                cqlType = field.getCqlType().getSet().getElement();
                break;
              default:
                // Can't happen, this has already been checked when building the model
                throw new AssertionError(
                    "Unexpected type for CONTAINS operator " + field.getCqlType());
            }
            break;
          default:
            cqlType = field.getCqlType();
            break;
        }
        Value cqlValue = toCqlValue(graphqlValue, cqlType, keyspace);
        result.add(condition.build(cqlValue));
      }
    }
    return activeConditions;
  }
}
