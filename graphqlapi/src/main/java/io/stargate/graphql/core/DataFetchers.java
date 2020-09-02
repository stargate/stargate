package io.stargate.graphql.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.stargate.auth.AuthenticationService;
import io.stargate.auth.StoredCredentials;
import io.stargate.auth.UnauthorizedException;
import io.stargate.coordinator.Coordinator;
import io.stargate.db.ClientState;
import io.stargate.db.Persistence;
import io.stargate.db.QueryState;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.Keyspace;
import io.stargate.db.datastore.schema.Table;
import io.stargate.graphql.graphqlservlet.HTTPAwareContextImpl;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.SelectedField;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class DataFetchers {
    private final NameMapping nameMapping;
    private final Coordinator coordinator;
    private final Persistence persistence;
    private AuthenticationService authenticationService;

    public DataFetchers(Coordinator coordinator, Keyspace keyspace, NameMapping nameMapping, AuthenticationService authenticationService) {
        this.coordinator = coordinator;
        this.persistence = coordinator.getPersistence();
        this.authenticationService = authenticationService;

        this.nameMapping = nameMapping;
    }

    abstract class AbstractMutationDataFetcher implements DataFetcher {
        protected final Table table;
        public AbstractMutationDataFetcher(Table table) {
            this.table = table;
        }

        @Override
        public Object get(DataFetchingEnvironment environment) throws Exception {
            HTTPAwareContextImpl httpAwareContext = environment.getContext();
//            if (httpAwareContext.getUserOrRole() != null) {
//                statement.setOutgoingPayload(ImmutableMap.of("ProxyExecute", ByteBuffer.wrap(httpAwareContext.getUserOrRole().getBytes())));
//            }

            String token = httpAwareContext.getAuthToken();
            StoredCredentials storedCredentials = authenticationService.validateToken(token);
            ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
            QueryState queryState = persistence.newQueryState(clientState);
            DataStore dataStore = persistence.newDataStore(queryState, null);

            String statement = buildStatement(table, environment, dataStore);
            CompletableFuture<ResultSet> rs = dataStore.query(statement);
            ResultSet resultSet = rs.get();

            ImmutableMap.Builder resultMap = ImmutableMap.builder();
            resultMap.put("value", environment.getArgument("value"));

            return resultMap.build();
        }

        public abstract String buildStatement(Table table, DataFetchingEnvironment environment, DataStore dataStore);
    }

    class InsertMutationDataFetcher extends AbstractMutationDataFetcher {
        public InsertMutationDataFetcher(Table table) {
            super(table);
        }

        @Override
        public String buildStatement(Table table, DataFetchingEnvironment environment, DataStore dataStore) {
            Insert insert = QueryBuilder.insertInto(table.keyspace(), table.name())
                    .values(buildInsertValues(environment));

            if (environment.containsArgument("ifNotExists") && environment.getArgument("ifNotExists") != null
                    && (Boolean) environment.getArgument("ifNotExists")) {
                insert = insert.ifNotExists();
            }
            if (environment.containsArgument("options") && environment.getArgument("options") != null) {
                Map<String, Object> options = environment.getArgument("options");
                if (options.containsKey("ttl") && options.get("ttl") != null) {
                    insert = insert.usingTtl((Integer)options.get("ttl"));
                }
                if (options.containsKey("consistency") && options.get("consistency") != null) {
//                    insert.setConsistencyLevel(ConsistencyLevel.valueOf(options.get("consistency").toString()));
                }
                if (options.containsKey("serialConsistency") && options.get("serialConsistency") != null) {
//                    insert.setSerialConsistencyLevel(ConsistencyLevel.valueOf(options.get("serialConsistency").toString()));
                }
            }

            return insert.asCql();
        }

        private Map<String, Term> buildInsertValues(DataFetchingEnvironment environment) {
            Map<String, Object> value = environment.getArgument("value");
            Preconditions.checkNotNull(value, "Insert statement must contain at least one field");

            Map<String, Term> insertMap = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : value.entrySet()) {
                insertMap.put(getDBColumnName(table, entry.getKey()),  literal(entry.getValue()));
            }
            return insertMap;
        }
    }

    class UpdateMutationDataFetcher extends AbstractMutationDataFetcher {

        public UpdateMutationDataFetcher(Table table) {
            super(table);
        }

        @Override
        public String buildStatement(Table table, DataFetchingEnvironment environment, DataStore dataStore) {
            UpdateStart updateStart = QueryBuilder.update(table.keyspace(), table.name());

            if (environment.containsArgument("options") && environment.getArgument("options") != null) {
                Map<String, Object> options = environment.getArgument("options");
                if (options.containsKey("ttl") && options.get("ttl") != null) {
                    updateStart = updateStart.usingTtl((Integer) options.get("ttl"));
                }
            }

            Update update = updateStart
                    .set(buildAssignments(table, environment))
                    .where(buildPkCKWhere(table, environment))
                    .if_(buildIfConditions(table, environment.getArgument("ifCondition")));

            if (environment.containsArgument("ifExists") && environment.getArgument("ifExists") != null
            && (Boolean) environment.getArgument("ifExists")) {
                update = update.ifExists();
            }

            if (environment.containsArgument("options") && environment.getArgument("options") != null) {
                Map<String, Object> options = environment.getArgument("options");
                if (options.containsKey("consistency") && options.get("consistency") != null) {
//                    update.setConsistencyLevel(ConsistencyLevel.valueOf(options.get("consistency").toString()));
                }
                if (options.containsKey("serialConsistency") && options.get("consistency") != null) {
//                    update.setSerialConsistencyLevel(ConsistencyLevel.valueOf(options.get("serialConsistency").toString()));
                }
            }

            return update.asCql();
        }

        private List<Relation> buildPkCKWhere(Table table, DataFetchingEnvironment environment) {
            Map<String, Object> value = environment.getArgument("value");
            List<Relation> relations = new ArrayList<>();

            for (Map.Entry<String, Object> entry : value.entrySet()) {
                Column columnMetadata = table.column(getDBColumnName(table, entry.getKey()));
                if (table.partitionKeyColumns().contains(columnMetadata) ||
                        table.clusteringKeyColumns().contains(columnMetadata)) {
                    relations.add(Relation.column(getDBColumnName(table, entry.getKey())).isEqualTo(literal(entry.getValue())));
                }
            }
            return relations;
        }

        private List<Assignment> buildAssignments(Table table, DataFetchingEnvironment environment) {
            Map<String, Object> value = environment.getArgument("value");
            List<Assignment> assignments = new ArrayList<>();
            for (Map.Entry<String, Object> entry : value.entrySet()) {
                Column columnMetadata = table.column(getDBColumnName(table, entry.getKey()));
                if (!(table.partitionKeyColumns().contains(columnMetadata) ||
                        table.clusteringKeyColumns().contains(columnMetadata))) {
                    assignments.add(Assignment.setColumn(getDBColumnName(table, entry.getKey()), literal(entry.getValue())));
                }
            }
            return assignments;
        }
    }

    class DeleteMutationDataFetcher extends AbstractMutationDataFetcher {

        public DeleteMutationDataFetcher(Table table) {
            super(table);
        }

        @Override
        public String buildStatement(Table table, DataFetchingEnvironment environment, DataStore dataStore) {
            Delete delete = QueryBuilder.deleteFrom(table.keyspace(), table.name())
                     .where(buildClause(table, environment))
                    .if_(buildIfConditions(table, environment.getArgument("ifCondition")));

            if (environment.containsArgument("ifExists") && environment.getArgument("ifExists") != null
                    && (Boolean) environment.getArgument("ifExists")) {
                delete = delete.ifExists();
            }

            if (environment.containsArgument("options") && environment.getArgument("options") != null) {
                Map<String, Object> options = environment.getArgument("options");
                if (options.containsKey("consistency")) {
//                    delete.setConsistencyLevel(ConsistencyLevel.valueOf(options.get("consistency").toString()));
                }
                if (options.containsKey("serialConsistency")) {
//                    delete.setSerialConsistencyLevel(ConsistencyLevel.valueOf(options.get("serialConsistency").toString()));
                }
            }

            return delete.asCql();
        }
    }

    class QueryDataFetcher implements DataFetcher {
        private final Table table;

        public QueryDataFetcher(Table table) {
            this.table = table;
        }

        public Object get(DataFetchingEnvironment environment) throws ExecutionException, InterruptedException, UnauthorizedException {
            String statement = buildQuery(environment);
            HTTPAwareContextImpl httpAwareContext = environment.getContext();
//            if (httpAwareContext.getUserOrRole() != null) {
//                statement.setOutgoingPayload(ImmutableMap.of("ProxyExecute", ByteBuffer.wrap(httpAwareContext.getUserOrRole().getBytes())));
//            }


            String token = httpAwareContext.getAuthToken();
            StoredCredentials storedCredentials = authenticationService.validateToken(token);
            ClientState clientState = persistence.newClientState(storedCredentials.getRoleName());
            QueryState queryState = persistence.newQueryState(clientState);
            DataStore dataStore = persistence.newDataStore(queryState, null);

            CompletableFuture<ResultSet> rs = dataStore.query(statement);
            ResultSet resultSet = rs.get();

            List<Map<String, Object>> results = new ArrayList<>();
            for (Row row : resultSet.rows()) {
                results.add(row2Map(row));
            }

            return ImmutableMap.of("values", results);
        }


        public Map<String, Object> row2Map(Row row) {
            List<Column> defs = row.columns();
            Map<String, Object> map = new HashMap<>(defs.size());
            for (Column column : defs) {
                if (row.has(column)) {
                    Column columnMetadata = table.column(column.name());
                    map.put(nameMapping.getColumnName(table).get(columnMetadata), transformObjectToJavaObject(row.getValue(column.name())));
                }
            }
            return map;
        }

        private Object transformObjectToJavaObject(Object o) {
            if (o instanceof Object[]) {
                return new ArrayList(Arrays.asList((Object[]) o));
            } else {
                return o;
            }
        }

        private String buildQuery(DataFetchingEnvironment environment) {
            Select select = QueryBuilder.selectFrom(table.keyspace(), table.name())
                    .columns(buildQueryColumns(environment))
                    .where(buildClause(table, environment))
                    .orderBy(buildOrderBy(environment));

            if (environment.containsArgument("options")) {
                Map<String, Object> options = environment.getArgument("options");
                if (options.containsKey("limit")) {
                    select = select.limit((Integer) options.get("limit"));
                }
                if (options.containsKey("pageSize")) {
//                    from.setFetchSize((Integer)options.get("pageSize"));
                }
                if (options.containsKey("pageState")) {
//                    select = select.whereRaw(PagingState.fromString((String)options.get("pageState")));
                }
                if (options.containsKey("consistency")) {
//                    select.setConsistencyLevel(ConsistencyLevel.valueOf((String)options.get("pageState")));
                }
            }

            return select.asCql();

        }

        private Map<String, ClusteringOrder> buildOrderBy(DataFetchingEnvironment environment) {
            if (environment.containsArgument("orderBy")) {
                Map<String, ClusteringOrder> orderMap = new LinkedHashMap<>();
                List<String> orderList = environment.getArgument("orderBy");
                for (int i = 0; i < orderList.size(); i++) {
                    String order = orderList.get(i);
                    int split = order.lastIndexOf("_");
                    String column = order.substring(0, split);
                    boolean desc = order.substring(split + 1).equals("DESC");
                    orderMap.put(getDBColumnName(table, column), desc ? ClusteringOrder.DESC : ClusteringOrder.ASC);
                }
                return orderMap;
            }

            return ImmutableMap.of();
        }

        private List<String> buildQueryColumns(DataFetchingEnvironment environment) {
            if (environment.getSelectionSet().contains("values")) {
                SelectedField field = environment.getSelectionSet().getField("values");
                List<String> fields = new ArrayList<>();
                for (SelectedField selectedField : field.getSelectionSet().getFields()) {
                    if ("__typename".equals(selectedField.getName())) {
                        continue;
                    }

                    String column = getDBColumnName(table, selectedField.getName());
                    if (table.column(column) != null) {
                        fields.add(column);
                    }
                }
                return fields;
            }

            return ImmutableList.of();
        }
    }

    private List<Condition> buildIfConditions(Table table, Map<String, Map<String, Object>> columnList) {
        if (columnList == null) {
            return ImmutableList.of();
        }
        List<Condition> clause = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
            for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
                switch (condition.getKey()) {
                    case "eq":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).isEqualTo(literal(condition.getValue())));
                        break;
                    case "notEq":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).isNotEqualTo(literal(condition.getValue())));
                        break;
                    case "gt":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).isGreaterThan(literal(condition.getValue())));
                        break;
                    case "gte":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).isGreaterThanOrEqualTo(literal(condition.getValue())));
                        break;
                    case "lt":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).isLessThan(literal(condition.getValue())));
                        break;
                    case "lte":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).isLessThanOrEqualTo(literal(condition.getValue())));
                        break;
                    case "in":
                        clause.add(Condition.column(getDBColumnName(table, clauseEntry.getKey())).in(buildListLiterals(condition.getValue())));
                        break;
                    default:
                }
            }
        }
        return clause;
    }

    private List<Relation> buildFilterConditions(Table table, Map<String, Map<String, Object>> columnList) {
        if (columnList == null) {
            return ImmutableList.of();
        }
        List<Relation> relations = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> clauseEntry : columnList.entrySet()) {
            for (Map.Entry<String, Object> condition : clauseEntry.getValue().entrySet()) {
                switch (condition.getKey()) {
                    case "eq":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).isEqualTo(literal(condition.getValue())));
                        break;
                    case "notEq":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).isNotEqualTo(literal(condition.getValue())));
                        break;
                    case "gt":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).isGreaterThan(literal(condition.getValue())));
                        break;
                    case "gte":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).isGreaterThanOrEqualTo(literal(condition.getValue())));
                        break;
                    case "lt":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).isLessThan(literal(condition.getValue())));
                        break;
                    case "lte":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).isLessThanOrEqualTo(literal(condition.getValue())));
                        break;
                    case "in":
                        relations.add(Relation.column(getDBColumnName(table, clauseEntry.getKey())).in(buildListLiterals(condition.getValue())));
                        break;
                    default:
                }
            }
        }
        return relations;
    }

    private List<Term> buildListLiterals(Object o) {
        List<Term> literals = new ArrayList();
        if (o instanceof List) {
            List values = (List) o;
            for (Object value : values) {
                literals.add(literal(value));
            }
        } else {
            literals.add(literal(o));
        }
        return literals;
    }


    private List<Relation> buildClause(Table table, DataFetchingEnvironment environment) {
        if (environment.containsArgument("filter")) {
            Map<String, Map<String, Object>> columnList = environment.getArgument("filter");
            return buildFilterConditions(table, columnList);
        } else {
            Map<String, Object> value = environment.getArgument("value");
            List<Relation> relations = new ArrayList<>();
            if (value == null) return ImmutableList.of();

            for (Map.Entry<String, Object> entry : value.entrySet()) {
                relations.add(Relation.column(getDBColumnName(table, entry.getKey())).isEqualTo(literal(entry.getValue())));
            }
            return relations;
        }
    }

    public String getDBColumnName(Table table, String column) {
        return nameMapping.getColumnName(table).inverse().get(column).name();
    }
}