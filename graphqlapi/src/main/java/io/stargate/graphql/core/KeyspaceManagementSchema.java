package io.stargate.graphql.core;

import java.util.HashMap;

import io.stargate.auth.AuthenticationService;
import io.stargate.coordinator.Coordinator;
import io.stargate.graphql.fetchers.AlterTableAddFetcher;
import io.stargate.graphql.fetchers.AlterTableDropFetcher;
import io.stargate.graphql.fetchers.CreateTableDataFetcher;
import io.stargate.graphql.fetchers.DropTableFetcher;
import io.stargate.graphql.fetchers.KeyspaceFetcher;
import io.stargate.graphql.fetchers.SchemaDataFetcherFactory;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;

import static graphql.schema.GraphQLList.list;
import static graphql.schema.GraphQLNonNull.nonNull;

public class KeyspaceManagementSchema {
    private final HashMap<String, GraphQLType> objects;
    private final Coordinator coordinator;
    private AuthenticationService authenticationService;
    private SchemaDataFetcherFactory schemaDataFetcherFactory;

    public KeyspaceManagementSchema(Coordinator coordinator, AuthenticationService authenticationService) {
        this.coordinator = coordinator;
        this.objects = new HashMap<>();
        this.authenticationService = authenticationService;
        this.schemaDataFetcherFactory = new SchemaDataFetcherFactory(coordinator, authenticationService);
    }

    public GraphQLSchema.Builder build() {
        GraphQLSchema.Builder builder = new GraphQLSchema.Builder();
        builder.mutation(buildMutation(buildCreateTable(), buildAlterTableAdd(), buildAlterTableDrop(), buildDrop()));
        builder.query(buildQuery(buildKeyspaceByName(), buildKeyspaces()));
        return builder;
    }

    private GraphQLFieldDefinition buildAlterTableAdd() {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("alterTableAdd")
                .argument(GraphQLArgument.newArgument()
                        .name("keyspaceName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("tableName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("toAdd")
                        .type(nonNull(list(buildColumnInput()))))
                .type(Scalars.GraphQLBoolean)
                .dataFetcher(schemaDataFetcherFactory.createSchemaFetcher(AlterTableAddFetcher.class.getName()))
                .build();
    }

    private GraphQLFieldDefinition buildAlterTableDrop() {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("alterTableDrop")
                .argument(GraphQLArgument.newArgument()
                        .name("keyspaceName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("tableName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("toDrop")
                        .type(nonNull(list(Scalars.GraphQLString))))
                .type(Scalars.GraphQLBoolean)
                .dataFetcher(schemaDataFetcherFactory.createSchemaFetcher(AlterTableDropFetcher.class.getName()))
                .build();
    }

    private GraphQLFieldDefinition buildDrop() {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("dropTable")
                .argument(GraphQLArgument.newArgument()
                        .name("keyspaceName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("tableName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("ifExists")
                        .type(Scalars.GraphQLBoolean))
                .type(Scalars.GraphQLBoolean)
                .dataFetcher(schemaDataFetcherFactory.createSchemaFetcher(DropTableFetcher.class.getName()))
                .build();
    }
    private GraphQLObjectType buildQuery(GraphQLFieldDefinition... defs) {
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Query");
        for (GraphQLFieldDefinition def : defs) {
            builder.field(def);
        }
        return builder.build();
    }

    private GraphQLFieldDefinition buildKeyspaceByName() {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("keyspace")
                .argument(GraphQLArgument.newArgument()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .type(buildKeyspace())
                .dataFetcher(new KeyspaceFetcher(coordinator, authenticationService).new KeyspaceByNameFetcher())
                .build();
    }

    private GraphQLObjectType buildKeyspace() {
        return register(GraphQLObjectType.newObject()
                .name("Keyspace")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("dcs")
                        .type(list(buildDataCenter())))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("table")
                        .argument(GraphQLArgument.newArgument()
                                .name("name")
                                .type(nonNull(Scalars.GraphQLString))
                                .build())
                        .type(buildTableType()))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("tables")
                        .type(list(buildTableType())))
                .build());
    }

    private GraphQLObjectType buildTableType() {
        return register(GraphQLObjectType.newObject()
                .name("Table")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("columns")
                        .type(list(buildColumnType())))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .build());
    }

    private GraphQLType buildColumnType() {
        return register(GraphQLObjectType.newObject()
                .name("Column")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("kind")
                        .type(nonNull(buildColumnKind())))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("type")
                        .type(nonNull(buildDataType())))
                .build());
    }

    private GraphQLType buildDataType() {
        return register(GraphQLObjectType.newObject()
                .name("DataType")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("basic")
                        .type(nonNull(buildBasicType())))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("info")
                        .type(buildDataTypeInfo()))
                .build());
    }


    private GraphQLOutputType buildDataTypeInfo() {
        return register(GraphQLObjectType.newObject()
                .name("DataTypeInfo")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("name")
                        .type(Scalars.GraphQLString))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("subTypes")
                        .type(list(new GraphQLTypeReference("DataType"))))
                .build());
    }

    private GraphQLType buildColumnKind() {
        return register(GraphQLEnumType.newEnum()
                .name("ColumnKind")
                .value("COMPACT")
                .value("UNKNOWN")
                .value("PARTITION")
                .value("CLUSTERING")
                .value("REGULAR")
                .value("STATIC")
                .build());
    }

    private GraphQLObjectType buildDataCenter() {
        return register(GraphQLObjectType.newObject()
                .name("DataCenter")
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .field(GraphQLFieldDefinition.newFieldDefinition()
                        .name("replicas")
                        .type(nonNull(Scalars.GraphQLInt)))
                .build());
    }

    private GraphQLFieldDefinition buildKeyspaces() {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("keyspaces")
                .type(list(buildKeyspace()))
                .dataFetcher(new KeyspaceFetcher(coordinator, authenticationService).new KeyspacesFetcher())
                .build();
    }

    private GraphQLObjectType buildMutation(GraphQLFieldDefinition... defs) {
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject().name("Mutation");
        for (GraphQLFieldDefinition def : defs) {
            builder.field(def);
        }
        return builder.build();
    }

    private GraphQLFieldDefinition buildCreateTable() {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("createTable")
                .argument(GraphQLArgument.newArgument()
                        .name("keyspaceName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("tableName")
                        .type(nonNull(Scalars.GraphQLString)))
                .argument(GraphQLArgument.newArgument()
                        .name("partitionKeys")
                        .type(nonNull(list(buildColumnInput()))))
                .argument(GraphQLArgument.newArgument()
                        .name("clusteringKeys")
                        .type(list(buildClusteringKeyInput())))
                .argument(GraphQLArgument.newArgument()
                        .name("values")
                        .type(list(buildColumnInput())))
                .argument(GraphQLArgument.newArgument()
                        .name("ifNotExists")
                        .type(Scalars.GraphQLBoolean))
                .type(Scalars.GraphQLBoolean)
                .dataFetcher(schemaDataFetcherFactory.createSchemaFetcher(CreateTableDataFetcher.class.getName()))
                .build();
    }

    private GraphQLInputObjectType buildClusteringKeyInput() {
        return register(GraphQLInputObjectType.newInputObject()
                .name("ClusteringKeyInput")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("type")
                        .type(nonNull(buildDataTypeInput())))
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("order")
                        .type(Scalars.GraphQLString))
                .build());
    }

    private GraphQLInputObjectType buildColumnInput() {
        return register(GraphQLInputObjectType.newInputObject()
                .name("ColumnInput")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("name")
                        .type(nonNull(Scalars.GraphQLString)))
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("type")
                        .type(nonNull(buildDataTypeInput())))
                .build());
    }

    private GraphQLInputObjectType buildDataTypeInput() {
        return register(GraphQLInputObjectType.newInputObject()
                .name("DataTypeInput")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("info")
                        .type(buildDataTypeInfoInput()))
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("basic")
                        .type(nonNull(buildBasicType())))
                .build());
    }

    private GraphQLInputType buildBasicType() {
        return register(GraphQLEnumType.newEnum()
                .name("BasicType")
                .value("CUSTOM")
                .value("INT")
                .value("TIMEUUID")
                .value("TIMESTAMP")
                .value("UDT")
                .value("BIGINT")
                .value("TIME")
                .value("DURATION")
                .value("VARINT")
                .value("UUID")
                .value("BOOLEAN")
                .value("TINYINT")
                .value("SMALLINT")
                .value("INET")
                .value("ASCII")
                .value("DECIMAL")
                .value("BLOB")
                .value("LIST")
                .value("MAP")
                .value("VARCHAR")
                .value("TUPLE")
                .value("DOUBLE")
                .value("COUNTER")
                .value("DATE")
                .value("TEXT")
                .value("FLOAT")
                .value("SET")
                .build());
    }

    private GraphQLInputObjectType buildDataTypeInfoInput() {
        return register(GraphQLInputObjectType.newInputObject()
                .name("DataTypeInfoInput")
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("subTypes")
                        .type(list(new GraphQLTypeReference("DataTypeInput"))))
                .field(GraphQLInputObjectField.newInputObjectField()
                        .name("name")
                        .type(Scalars.GraphQLString))
                .build());
    }

    private <T> T register(GraphQLType object) {
        String name = getName(object);
        if (name == null) {
            throw new RuntimeException("Schema object has no name" + object);
        }

        Object o = objects.get(name);
        if (o == null) {
            objects.put(name, object);
            return (T) object;
        }
        return (T) o;
    }

    private String getName(GraphQLType object) {
        if (object instanceof GraphQLInputObjectType) {
            return ((GraphQLInputObjectType)object).getName();
        } else if (object instanceof GraphQLObjectType) {
            return ((GraphQLObjectType)object).getName();
        } else if (object instanceof GraphQLEnumType) {
            return ((GraphQLEnumType)object).getName();
        }
        return null;
    }
}