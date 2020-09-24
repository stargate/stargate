package io.stargate.graphql.core;

import static graphql.Scalars.GraphQLString;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.idl.SchemaPrinter;
import io.stargate.auth.AuthenticationService;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.db.datastore.schema.Table;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GqlKeyspaceSchema {
  private static final Logger log = LoggerFactory.getLogger(GqlKeyspaceSchema.class);

  private final DataFetchers fetcherFactory;
  private final Map<Column.ColumnType, GraphQLInputObjectType> filterInputTypes;
  private final Map<Table, GraphQLOutputType> entityResultMap = new HashMap<>();
  private final NameMapping nameMapping;
  private Set<Table> tables;

  private GraphQLInputType mutationOptions =
      GraphQLInputObjectType.newInputObject()
          .name("MutationOptions")
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("consistency")
                  .type(
                      GraphQLEnumType.newEnum()
                          .name("MutationConsistency")
                          .value("LOCAL_ONE")
                          .value("LOCAL_QUORUM")
                          .value("ALL")
                          .build())
                  .build())
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("serialConsistency")
                  .type(
                      GraphQLEnumType.newEnum()
                          .name("SerialConsistency")
                          .value("SERIAL")
                          .value("LOCAL_SERIAL")
                          .build())
                  .build())
          .field(
              GraphQLInputObjectField.newInputObjectField()
                  .name("ttl")
                  .type(Scalars.GraphQLInt)
                  .defaultValue(-1)
                  .build())
          .build();

  public GqlKeyspaceSchema(
      Persistence persistence, AuthenticationService authenticationService, Keyspace keyspace) {
    this.tables = keyspace.tables();

    this.nameMapping = new NameMapping(tables);
    this.fetcherFactory =
        new DataFetchers(persistence, keyspace, nameMapping, authenticationService);
    this.filterInputTypes = buildFilterInputTypes();
  }

  public GraphQLSchema.Builder build() {
    GraphQLSchema.Builder builder = new GraphQLSchema.Builder();

    List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();

    // Tables must be iterated one at a time. If a table is unfulfillable, it is skipped
    for (Table table : tables) {
      Set<GraphQLType> additionalTypes;
      List<GraphQLFieldDefinition> tableQueryField;
      List<GraphQLFieldDefinition> tableMutationFields;

      try {
        additionalTypes = buildTypesForTable(table);
        tableQueryField = buildQuery(table);
        tableMutationFields = buildMutations(table);
      } catch (Exception e) {
        log.warn("Skipping table " + table.name());
        continue;
      }

      builder.additionalTypes(additionalTypes);
      queryFields.addAll(tableQueryField);
      mutationFields.addAll(tableMutationFields);
    }

    if (queryFields.isEmpty()) {
      GraphQLFieldDefinition emptyQueryField =
          GraphQLFieldDefinition.newFieldDefinition()
              .name("__keyspaceEmptyQuery")
              .description("Placeholder query that is exposed when a keyspace is empty.")
              .type(Scalars.GraphQLBoolean)
              .dataFetcher((d) -> true)
              .build();
      queryFields.add(emptyQueryField);
    }

    if (mutationFields.isEmpty()) {
      GraphQLFieldDefinition emptyMutationField =
          GraphQLFieldDefinition.newFieldDefinition()
              .name("__keyspaceEmptyMutation")
              .description("Placeholder mutation that is exposed when a keyspace is empty.")
              .type(Scalars.GraphQLBoolean)
              .dataFetcher((d) -> true)
              .build();
      mutationFields.add(emptyMutationField);
    }

    builder.additionalType(buildQueryOptionsInputType());
    builder.query(buildQueries(queryFields));
    builder.mutation(buildMutationRoot(mutationFields));
    return builder;
  }

  private GraphQLObjectType buildMutationRoot(List<GraphQLFieldDefinition> mutationFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Mutation");
    for (GraphQLFieldDefinition mutation : mutationFields) {
      builder.field(mutation);
    }

    return builder.build();
  }

  private GraphQLObjectType buildQueries(List<GraphQLFieldDefinition> queryFields) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");

    for (GraphQLFieldDefinition fieldDefinition : queryFields) {
      builder.field(fieldDefinition);
    }

    return builder.build();
  }

  private List<GraphQLFieldDefinition> buildQuery(Table table) {
    GraphQLFieldDefinition query =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(CaseUtil.toLowerCamel(table.name()))
            .argument(
                GraphQLArgument.newArgument()
                    .name("value")
                    .type(
                        new GraphQLTypeReference(nameMapping.getEntityName().get(table) + "Input")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(
                        new GraphQLTypeReference(
                            nameMapping.getEntityName().get(table) + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(
                        new GraphQLList(
                            new GraphQLTypeReference(
                                nameMapping.getEntityName().get(table) + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(fetcherFactory.new QueryDataFetcher(table))
            .build();

    GraphQLFieldDefinition filterQuery =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(CaseUtil.toLowerCamel(table.name()) + "Filter")
            .deprecate("No longer supported. Use root type instead.")
            .argument(
                GraphQLArgument.newArgument()
                    .name("filter")
                    .type(
                        new GraphQLTypeReference(
                            nameMapping.getEntityName().get(table) + "FilterInput")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("orderBy")
                    .type(
                        new GraphQLList(
                            new GraphQLTypeReference(
                                nameMapping.getEntityName().get(table) + "Order"))))
            .argument(
                GraphQLArgument.newArgument()
                    .name("options")
                    .type(new GraphQLTypeReference("QueryOptions")))
            .type(buildEntityResultOutput(table))
            .dataFetcher(fetcherFactory.new QueryDataFetcher(table))
            .build();

    return ImmutableList.of(query, filterQuery);
  }

  private List<GraphQLFieldDefinition> buildMutations(Table table) {
    List<GraphQLFieldDefinition> mutationFields = new ArrayList<>();
    mutationFields.add(buildDelete(table));
    mutationFields.add(buildInsert(table));
    mutationFields.add(buildUpdate(table));

    return mutationFields;
  }

  private Set<GraphQLType> buildTypesForTable(Table table) {
    Set<GraphQLType> additionalTypes = new HashSet<>();

    additionalTypes.add(buildType(table));
    additionalTypes.add(buildInputType(table));
    additionalTypes.add(buildOrderType(table));
    additionalTypes.add(buildMutationResult(table));
    additionalTypes.add(buildFilterInput(table));
    return additionalTypes;
  }

  private GraphQLType buildFilterInput(Table table) {
    return GraphQLInputObjectType.newInputObject()
        .name(nameMapping.getEntityName().get(table) + "FilterInput")
        .fields(buildFilterInputFields(table))
        .build();
  }

  private GraphQLFieldDefinition buildUpdate(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("update" + nameMapping.getEntityName().get(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(
                            nameMapping.getEntityName().get(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifCondition")
                .type(
                    new GraphQLTypeReference(
                        nameMapping.getEntityName().get(table) + "FilterInput")))
        .argument(GraphQLArgument.newArgument().name("options").type(mutationOptions))
        .type(new GraphQLTypeReference(nameMapping.getEntityName().get(table) + "MutationResult"))
        .dataFetcher(fetcherFactory.new UpdateMutationDataFetcher(table))
        .build();
  }

  private GraphQLFieldDefinition buildInsert(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("insert" + nameMapping.getEntityName().get(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(
                            nameMapping.getEntityName().get(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifNotExists").type(Scalars.GraphQLBoolean))
        .argument(GraphQLArgument.newArgument().name("options").type(mutationOptions))
        .type(new GraphQLTypeReference(nameMapping.getEntityName().get(table) + "MutationResult"))
        .dataFetcher(fetcherFactory.new InsertMutationDataFetcher(table))
        .build();
  }

  private GraphQLFieldDefinition buildDelete(Table table) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name("delete" + nameMapping.getEntityName().get(table))
        .argument(
            GraphQLArgument.newArgument()
                .name("value")
                .type(
                    new GraphQLNonNull(
                        new GraphQLTypeReference(
                            nameMapping.getEntityName().get(table) + "Input"))))
        .argument(GraphQLArgument.newArgument().name("ifExists").type(Scalars.GraphQLBoolean))
        .argument(
            GraphQLArgument.newArgument()
                .name("ifCondition")
                .type(
                    new GraphQLTypeReference(
                        nameMapping.getEntityName().get(table) + "FilterInput")))
        .argument(GraphQLArgument.newArgument().name("options").type(mutationOptions))
        .type(new GraphQLTypeReference(nameMapping.getEntityName().get(table) + "MutationResult"))
        .dataFetcher(fetcherFactory.new DeleteMutationDataFetcher(table))
        .build();
  }

  private Map<Column.ColumnType, GraphQLInputObjectType> buildFilterInputTypes() {
    GraphQLInputObjectType stringFilterInput = filterInputType(Scalars.GraphQLString);
    GraphQLInputObjectType intFilterInput = filterInputType(Scalars.GraphQLInt);
    GraphQLInputObjectType floatFilterInput = filterInputType(Scalars.GraphQLFloat);
    GraphQLInputObjectType uuidFilterInput = filterInputType(CustomScalar.UUID.getGraphQLScalar());
    GraphQLInputObjectType timestampFilterInput =
        filterInputType(CustomScalar.TIMESTAMP.getGraphQLScalar());
    GraphQLInputObjectType timeUUIDFilterInput =
        filterInputType(CustomScalar.TIMEUUID.getGraphQLScalar());
    GraphQLInputObjectType inetFilterInput = filterInputType(CustomScalar.INET.getGraphQLScalar());
    GraphQLInputObjectType bigIntFilterInput =
        filterInputType(CustomScalar.BIGINT.getGraphQLScalar());
    GraphQLInputObjectType decimalFilterInput =
        filterInputType(CustomScalar.DECIMAL.getGraphQLScalar());
    GraphQLInputObjectType varintFilterInput =
        filterInputType(CustomScalar.VARINT.getGraphQLScalar());
    GraphQLInputObjectType blobFilterInput = filterInputType(CustomScalar.BLOB.getGraphQLScalar());

    return ImmutableMap.<Column.ColumnType, GraphQLInputObjectType>builder()
        .put(Column.Type.Int, intFilterInput)
        .put(Column.Type.Smallint, intFilterInput)
        .put(Column.Type.Tinyint, intFilterInput)
        .put(Column.Type.Text, stringFilterInput)
        .put(Column.Type.Varchar, stringFilterInput)
        .put(Column.Type.Float, floatFilterInput)
        .put(Column.Type.Double, floatFilterInput)
        .put(Column.Type.Uuid, uuidFilterInput)
        .put(Column.Type.Timestamp, timestampFilterInput)
        .put(Column.Type.Timeuuid, timeUUIDFilterInput)
        .put(Column.Type.Inet, inetFilterInput)
        .put(Column.Type.Bigint, bigIntFilterInput)
        .put(Column.Type.Decimal, decimalFilterInput)
        .put(Column.Type.Varint, varintFilterInput)
        .put(Column.Type.Blob, blobFilterInput)
        .build();
  }

  private List<GraphQLInputObjectField> buildFilterInputFields(Table table) {
    List<GraphQLInputObjectField> fields = new ArrayList<>();
    for (Column columnMetadata : table.columns()) {
      if (filterInputTypes.get(columnMetadata.type()) != null) {
        fields.add(
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getColumnName(table).get(columnMetadata))
                .type(getFilterInputTypeRef(columnMetadata.type()))
                .build());
      }
    }

    Preconditions.checkState(
        !fields.isEmpty(), "Could not generate an input type for table, skipping.");

    return fields;
  }

  private GraphQLInputType getFilterInputTypeRef(Column.ColumnType dataType) {
    return filterInputTypes.get(dataType);
  }

  private static GraphQLInputObjectType filterInputType(GraphQLScalarType type) {
    return GraphQLInputObjectType.newInputObject()
        .name(type.getName() + "FilterInput")
        .field(GraphQLInputObjectField.newInputObjectField().name("eq").type(type))
        .field(GraphQLInputObjectField.newInputObjectField().name("notEq").type(type))
        .field(GraphQLInputObjectField.newInputObjectField().name("gt").type(type))
        .field(GraphQLInputObjectField.newInputObjectField().name("gte").type(type))
        .field(GraphQLInputObjectField.newInputObjectField().name("lt").type(type))
        .field(GraphQLInputObjectField.newInputObjectField().name("lte").type(type))
        .field(GraphQLInputObjectField.newInputObjectField().name("in").type(new GraphQLList(type)))
        .build();
  }

  private GraphQLOutputType buildMutationResult(Table table) {
    return GraphQLObjectType.newObject()
        .name(nameMapping.getEntityName().get(table) + "MutationResult")
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("applied")
                .type(Scalars.GraphQLBoolean))
        .field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name("value")
                .type(new GraphQLTypeReference(nameMapping.getEntityName().get(table))))
        .build();
  }

  private GraphQLType buildQueryOptionsInputType() {
    return GraphQLInputObjectType.newInputObject()
        .name("QueryOptions")
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("consistency")
                .type(
                    GraphQLEnumType.newEnum()
                        .name("QueryConsistency")
                        .value("LOCAL_ONE")
                        .value("LOCAL_QUORUM")
                        .value("ALL")
                        .value("SERIAL")
                        .value("LOCAL_SERIAL")
                        .build())
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("limit")
                .type(Scalars.GraphQLInt)
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("pageSize")
                .type(Scalars.GraphQLInt)
                .defaultValue(100)
                .build())
        .field(
            GraphQLInputObjectField.newInputObjectField()
                .name("pageState")
                .type(Scalars.GraphQLString)
                .build())
        .build();
  }

  private GraphQLType buildOrderType(Table table) {
    GraphQLEnumType.Builder input =
        GraphQLEnumType.newEnum().name(nameMapping.getEntityName().get(table) + "Order");
    for (Column columnMetadata : table.columns()) {
      input.value(nameMapping.getColumnName(table).get(columnMetadata) + "_DESC");
      input.value(nameMapping.getColumnName(table).get(columnMetadata) + "_ASC");
    }
    return input.build();
  }

  private GraphQLType buildInputType(Table table) {
    GraphQLInputObjectType.Builder input =
        GraphQLInputObjectType.newInputObject()
            .name(nameMapping.getEntityName().get(table) + "Input");
    for (Column columnMetadata : table.columns()) {
      try {
        GraphQLInputObjectField field =
            GraphQLInputObjectField.newInputObjectField()
                .name(nameMapping.getColumnName(table).get(columnMetadata))
                .type((GraphQLInputType) getGraphQLType(columnMetadata.type()))
                .build();
        input.field(field);
      } catch (Exception e) {

      }
    }
    return input.build();
  }

  private GraphQLOutputType buildEntityResultOutput(Table table) {
    if (entityResultMap.containsKey(table)) {
      return entityResultMap.get(table);
    }

    GraphQLOutputType entityResultType =
        GraphQLObjectType.newObject()
            .name(nameMapping.getEntityName().get(table) + "Result")
            .field(
                GraphQLFieldDefinition.newFieldDefinition().name("pageState").type(GraphQLString))
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("values")
                    .type(
                        new GraphQLList(
                            new GraphQLNonNull(
                                new GraphQLTypeReference(nameMapping.getEntityName().get(table))))))
            .build();

    entityResultMap.put(table, entityResultType);

    return entityResultType;
  }

  public GraphQLObjectType buildType(Table table) {
    GraphQLObjectType.Builder builder =
        GraphQLObjectType.newObject().name(nameMapping.getEntityName().get(table));
    for (Column columnMetadata : table.columns()) {
      try {
        GraphQLFieldDefinition.Builder fieldBuilder = buildOutputField(table, columnMetadata);
        builder.field(fieldBuilder.build());
      } catch (Exception e) {

      }
    }

    return builder.build();
  }

  private GraphQLFieldDefinition.Builder buildOutputField(Table table, Column columnMetadata) {
    return new GraphQLFieldDefinition.Builder()
        .name(nameMapping.getColumnName(table).get(columnMetadata))
        .type(getGraphQLType(columnMetadata.type()));
  }

  private GraphQLOutputType getGraphQLType(Column.ColumnType type) {
    switch (type.rawType()) {
        //            case CUSTOM:
        //                throw new RuntimeException("unknown data type");
      case Ascii:
        return CustomScalar.ASCII.getGraphQLScalar();
      case Bigint:
        return CustomScalar.BIGINT.getGraphQLScalar();
      case Blob:
        return CustomScalar.BLOB.getGraphQLScalar();
      case Boolean:
        return Scalars.GraphQLBoolean;
      case Counter:
        return CustomScalar.COUNTER.getGraphQLScalar();
      case Decimal:
        return CustomScalar.DECIMAL.getGraphQLScalar();
      case Double:
        return Scalars.GraphQLBigDecimal;
      case Float:
        return CustomScalar.FLOAT.getGraphQLScalar();
      case Int:
        return Scalars.GraphQLInt;
      case Text:
        return Scalars.GraphQLString;
      case Timestamp:
        return CustomScalar.TIMESTAMP.getGraphQLScalar();
      case Uuid:
        return CustomScalar.UUID.getGraphQLScalar();
      case Varchar:
        return Scalars.GraphQLString;
      case Varint:
        return CustomScalar.VARINT.getGraphQLScalar();
      case Timeuuid:
        return CustomScalar.TIMEUUID.getGraphQLScalar();
      case Inet:
        return CustomScalar.INET.getGraphQLScalar();
      case Date:
        return CustomScalar.DATE.getGraphQLScalar();
      case Time:
        return CustomScalar.TIME.getGraphQLScalar();
      case Smallint:
        return Scalars.GraphQLInt;
      case Tinyint:
        return Scalars.GraphQLInt;
        //            case Duration:
        //                return CustomScalar.DURATION.getGraphQLScalar();
      case List:
        return new GraphQLList(getGraphQLType(type.parameters().get(0)));
      case Map:
        break;
      case Set:
        return new GraphQLList(getGraphQLType(type.parameters().get(0)));
      case UDT:
        break;
      case Tuple:
        break;
    }

    throw new RuntimeException("Unsupported data type " + type.name());
  }

  public static String schema2String(GraphQLSchema schema) {
    SchemaPrinter printer = new SchemaPrinter();
    StringBuilder result = new StringBuilder();
    // Print out scalars specifically, they may be unordered so reorder them
    List<String> scalars = new ArrayList<>();

    Collections.sort(scalars);
    scalars.forEach(result::append);

    result.append("\n");
    result.append(printer.print(schema));

    return result.toString();
  }
}
