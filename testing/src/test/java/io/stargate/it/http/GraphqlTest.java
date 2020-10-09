package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloMutationCall;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.CustomTypeAdapter;
import com.apollographql.apollo.api.CustomTypeValue;
import com.apollographql.apollo.api.Error;
import com.apollographql.apollo.api.Mutation;
import com.apollographql.apollo.api.Operation;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.example.graphql.client.betterbotz.collections.GetCollectionsSimpleQuery;
import com.example.graphql.client.betterbotz.collections.InsertCollectionsSimpleMutation;
import com.example.graphql.client.betterbotz.collections.UpdateCollectionsSimpleMutation;
import com.example.graphql.client.betterbotz.orders.GetOrdersByValueQuery;
import com.example.graphql.client.betterbotz.orders.GetOrdersWithFilterQuery;
import com.example.graphql.client.betterbotz.products.DeleteProductsMutation;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery;
import com.example.graphql.client.betterbotz.products.InsertProductsMutation;
import com.example.graphql.client.betterbotz.products.UpdateProductsMutation;
import com.example.graphql.client.betterbotz.type.AUdtInput;
import com.example.graphql.client.betterbotz.type.BUdtInput;
import com.example.graphql.client.betterbotz.type.CollectionsSimpleInput;
import com.example.graphql.client.betterbotz.type.CustomType;
import com.example.graphql.client.betterbotz.type.InputKeyIntValueString;
import com.example.graphql.client.betterbotz.type.OrdersFilterInput;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.ProductsFilterInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import com.example.graphql.client.betterbotz.type.QueryConsistency;
import com.example.graphql.client.betterbotz.type.QueryOptions;
import com.example.graphql.client.betterbotz.type.StringFilterInput;
import com.example.graphql.client.betterbotz.type.UdtsInput;
import com.example.graphql.client.betterbotz.type.UuidFilterInput;
import com.example.graphql.client.betterbotz.udts.GetUdtsQuery;
import com.example.graphql.client.betterbotz.udts.InsertUdtsMutation;
import com.example.graphql.client.schema.AlterTableAddMutation;
import com.example.graphql.client.schema.AlterTableDropMutation;
import com.example.graphql.client.schema.CreateKeyspaceMutation;
import com.example.graphql.client.schema.CreateTableMutation;
import com.example.graphql.client.schema.DropTableMutation;
import com.example.graphql.client.schema.GetKeyspaceQuery;
import com.example.graphql.client.schema.GetKeyspacesQuery;
import com.example.graphql.client.schema.GetTableQuery;
import com.example.graphql.client.schema.GetTablesQuery;
import com.example.graphql.client.schema.type.BasicType;
import com.example.graphql.client.schema.type.ClusteringKeyInput;
import com.example.graphql.client.schema.type.ColumnInput;
import com.example.graphql.client.schema.type.DataTypeInput;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.http.HttpStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * To update these tests:
 *
 * <ul>
 *   <li>If the schema has changed, update the `schema.json` files in `src/main/graphql`. You can
 *       use the query in `src/main/resources/introspection.graphql` (paste it into the graphql
 *       playground at ${STARGATE_HOST}:8080/playground).
 *   <li>If there are new operations, create corresponding descriptors in
 *       `src/main/graphql/betterbotz` or `src/main/graphql/schema`. For betterbotz, there's a cql
 *       schema file at src/main/resources/betterbotz.cql
 *   <li>Run the apollo-client-maven-plugin, which reads the descriptors and generates the
 *       corresponding Java types: `mvn generate-sources` (an IDE rebuild should also work). You can
 *       see generated code in `target/generated-sources/graphql-client`.
 * </ul>
 */
@NotThreadSafe
public class GraphqlTest extends BaseOsgiIntegrationTest {

  private CqlSession session;
  private String keyspace;
  private static String authToken;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static String host = "http://" + getStargateHost();

  public GraphqlTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setup(ClusterConnectionInfo cluster) throws IOException {
    keyspace = "betterbotz";

    session =
        CqlSession.builder()
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(1))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(20))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
            .withLocalDatacenter(cluster.datacenter())
            .build();

    assertThat(
            session
                .execute(
                    String.format(
                        "create keyspace if not exists %s WITH replication = "
                            + "{'class': 'SimpleStrategy', 'replication_factor': 1 }",
                        keyspace))
                .wasApplied())
        .isTrue();

    assertThat(
            session
                .execute(
                    String.format(
                        "create table if not exists %s.%s "
                            + " ("
                            + "id uuid,"
                            + "name text,"
                            + "price decimal,"
                            + "created timestamp,"
                            + "prod_name text,"
                            + "customer_name text,"
                            + "description text,"
                            + "PRIMARY KEY ((id), name, price, created)"
                            + ")",
                        keyspace, "products"))
                .wasApplied())
        .isTrue();

    assertThat(
            session
                .execute(
                    String.format(
                        "create table if not exists %s.%s "
                            + " ("
                            + "prod_name text,"
                            + "customer_name text,"
                            + "id uuid,"
                            + "prod_id uuid,"
                            + "address text,"
                            + "description text,"
                            + "price decimal,"
                            + "sell_price decimal,"
                            + "PRIMARY KEY ((prod_name), customer_name)"
                            + ")",
                        keyspace, "orders"))
                .wasApplied())
        .isTrue();

    session.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s ("
                + "id uuid PRIMARY KEY,\n"
                + "list_value1 frozen<list<int>>,\n"
                + "list_value2 frozen<list<timeuuid>>,\n"
                + "set_value1 frozen<set<text>>,\n"
                + "map_value1 frozen<map<int, text>>,\n"
                + "map_value2 frozen<map<uuid, bigint>>,)",
            keyspace, "collections_simple"));

    session.execute(String.format("CREATE TYPE %s.b(i int)", keyspace));
    session.execute(String.format("CREATE TYPE %s.a(b frozen<b>)", keyspace));
    session.execute(
        String.format(
            "CREATE TABLE %s.udts(a frozen<a> PRIMARY KEY, bs list<frozen<b>>)", keyspace));

    PreparedStatement insert =
        session.prepare(
            String.format(
                "insert into %s.%s (id, prod_id, prod_name, description, price,"
                    + "sell_price, customer_name, address) values (?, ?, ?, ?, ?, ?, ?, ?)",
                keyspace, "orders"));

    assertThat(
            session
                .execute(
                    insert.bind(
                        UUID.fromString("792d0a56-bb46-4bc2-bc41-5f4a94a83da9"),
                        UUID.fromString("31047029-2175-43ce-9fdd-b3d568b19bb2"),
                        "Medium Lift Arms",
                        "Ordering some more arms for my construction bot.",
                        BigDecimal.valueOf(3199.99),
                        BigDecimal.valueOf(3119.99),
                        "Janice Evernathy",
                        "2101 Everplace Ave 3116"))
                .wasApplied())
        .isTrue();

    assertThat(
            session
                .execute(
                    insert.bind(
                        UUID.fromString("dd73afe2-9841-4ce1-b841-575b8be405c1"),
                        UUID.fromString("31047029-2175-43ce-9fdd-b3d568b19bb5"),
                        "Basic Task CPU",
                        "Ordering replacement CPUs.",
                        BigDecimal.valueOf(899.99),
                        BigDecimal.valueOf(900.82),
                        "John Doe",
                        "123 Main St 67890"))
                .wasApplied())
        .isTrue();

    initAuth();
  }

  @AfterEach
  public void teardown() {
    if (session != null) {
      session.close();
    }
  }

  private void initAuth() throws IOException {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }

  @Test
  public void getKeyspaces() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    GetKeyspacesQuery query = GetKeyspacesQuery.builder().build();

    CompletableFuture<GetKeyspacesQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetKeyspacesQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetKeyspacesQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getKeyspaces()).isPresent();

    List<GetKeyspacesQuery.Keyspace> keyspaces = result.getKeyspaces().get();
    assertThat(keyspaces)
        .extracting(GetKeyspacesQuery.Keyspace::getName)
        .anySatisfy(value -> assertThat(value).matches("system"));
  }

  @Test
  public void getKeyspace() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    GetKeyspaceQuery query = GetKeyspaceQuery.builder().name("system").build();

    CompletableFuture<GetKeyspaceQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetKeyspaceQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetKeyspaceQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getKeyspace()).isPresent();

    GetKeyspaceQuery.Keyspace keyspace = result.getKeyspace().get();
    assertThat(keyspace.getName()).isEqualTo("system");
    assertThat(keyspace.getTables()).isPresent();

    List<GetKeyspaceQuery.Table> tables = keyspace.getTables().get();

    assertThat(tables)
        .filteredOn(t -> t.getName().equals("peers"))
        .flatExtracting(c -> c.getColumns().orElseThrow(RuntimeException::new))
        .filteredOn(c -> c.getName().equals("schema_version"))
        .extracting(GetKeyspaceQuery.Column::getType)
        .anySatisfy(value -> assertThat(value.getBasic()).isEqualTo(BasicType.UUID));
  }

  @Test
  public void createKeyspace() throws Exception {
    String newKeyspaceName = "graphql_create_test";

    assertThat(
            session
                .execute(String.format("drop keyspace if exists %s", newKeyspaceName))
                .wasApplied())
        .isTrue();

    ApolloClient client = getApolloClient("/graphql-schema");
    CreateKeyspaceMutation mutation =
        CreateKeyspaceMutation.builder()
            .name(newKeyspaceName)
            .ifNotExists(true)
            .replicas(1)
            .build();

    CompletableFuture<CreateKeyspaceMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<CreateKeyspaceMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    CreateKeyspaceMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getCreateKeyspace()).hasValue(true);

    // Create a table in the new keyspace via CQL to validate that the keyspace is usable
    assertThat(
            session
                .execute(
                    String.format(
                        "create table %s.%s (id uuid, primary key (id))", newKeyspaceName, "test"))
                .wasApplied())
        .isTrue();
  }

  @Test
  public void getTables() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    GetTablesQuery query = GetTablesQuery.builder().keyspaceName("system").build();

    CompletableFuture<GetTablesQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetTablesQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetTablesQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getKeyspace()).isPresent();
    GetTablesQuery.Keyspace keyspace = result.getKeyspace().get();

    assertThat(keyspace.getTables()).isPresent();
    List<GetTablesQuery.Table> tables = keyspace.getTables().get();

    assertThat(tables)
        .extracting(GetTablesQuery.Table::getName)
        .anySatisfy(value -> assertThat(value).matches("local"));
    assertThat(tables)
        .extracting(GetTablesQuery.Table::getName)
        .anySatisfy(value -> assertThat(value).matches("peers"));
  }

  @Test
  public void getTable() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");

    String keyspaceName = "system";
    String tableName = "local";
    GetTableQuery.Table table = getTable(client, keyspaceName, tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns)
        .filteredOn(c -> c.getName().equals("listen_address"))
        .extracting(GetTableQuery.Column::getType)
        .anySatisfy(value -> assertThat(value.getBasic()).isEqualTo(BasicType.INET));
  }

  public GetTableQuery.Table getTable(ApolloClient client, String keyspaceName, String tableName)
      throws ExecutionException, InterruptedException {
    GetTableQuery query =
        GetTableQuery.builder().keyspaceName(keyspaceName).tableName(tableName).build();

    CompletableFuture<GetTableQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetTableQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetTableQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getKeyspace()).isPresent();

    GetTableQuery.Keyspace keyspace = result.getKeyspace().get();
    assertThat(keyspace.getName()).isEqualTo(keyspaceName);
    assertThat(keyspace.getTable()).isPresent();

    GetTableQuery.Table table = keyspace.getTable().get();
    assertThat(table.getName()).isEqualTo(tableName);

    return table;
  }

  @Test
  public void createTable() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    String tableName = "tbl_createtable_" + System.currentTimeMillis();

    List<ColumnInput> partitionKeys = new ArrayList<>();
    partitionKeys.add(
        ColumnInput.builder()
            .name("id")
            .type(DataTypeInput.builder().basic(BasicType.UUID).build())
            .build());
    List<ColumnInput> values = new ArrayList<>();
    values.add(
        ColumnInput.builder()
            .name("lastname")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());
    values.add(
        ColumnInput.builder()
            .name("firstName")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());

    createTable(client, keyspace, tableName, partitionKeys, values);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns)
        .filteredOn(c -> c.getName().equals("id"))
        .extracting(GetTableQuery.Column::getType)
        .anySatisfy(value -> assertThat(value.getBasic()).isEqualTo(BasicType.UUID));
  }

  @Test
  @DisplayName("Should create table with clustering keys")
  public void createTableWithClusteringKey() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    String tableName = "tbl_createtable_with_ck_" + System.currentTimeMillis();

    CreateTableMutation.Builder builder =
        CreateTableMutation.builder()
            .partitionKeys(
                ImmutableList.of(
                    ColumnInput.builder()
                        .name("pk1")
                        .type(DataTypeInput.builder().basic(BasicType.INT).build())
                        .build()))
            .clusteringKeys(
                ImmutableList.of(
                    ClusteringKeyInput.builder()
                        .name("ck1")
                        .type(DataTypeInput.builder().basic(BasicType.TIMEUUID).build())
                        .order(null)
                        .build(),
                    ClusteringKeyInput.builder()
                        .name("ck2")
                        .type(DataTypeInput.builder().basic(BasicType.BIGINT).build())
                        .order("DESC")
                        .build()))
            .values(
                ImmutableList.of(
                    ColumnInput.builder()
                        .name("value1")
                        .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
                        .build()));

    createTable(client, tableName, builder);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns).filteredOn(c -> c.getName().equals("pk1")).hasSize(1);
    assertThat(columns)
        .filteredOn(c -> c.getName().equals("ck1") || c.getName().equals("ck2"))
        .hasSize(2);
  }

  private GetTableQuery.Table createTable(
      ApolloClient client,
      String keyspaceName,
      String tableName,
      List<ColumnInput> partitionKeys,
      List<ColumnInput> values)
      throws ExecutionException, InterruptedException {
    return createTable(
        client,
        tableName,
        CreateTableMutation.builder()
            .keyspaceName(keyspaceName)
            .partitionKeys(partitionKeys)
            .values(values));
  }

  private GetTableQuery.Table createTable(
      ApolloClient client, String tableName, CreateTableMutation.Builder mutationBuilder)
      throws ExecutionException, InterruptedException {

    CreateTableMutation mutation =
        mutationBuilder.keyspaceName(keyspace).tableName(tableName).build();
    CompletableFuture<CreateTableMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<CreateTableMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    CreateTableMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getCreateTable()).hasValue(true);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    return table;
  }

  @Test
  public void dropTable() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    String tableName = "tbl_droptable_" + System.currentTimeMillis();

    List<ColumnInput> partitionKeys = new ArrayList<>();
    partitionKeys.add(
        ColumnInput.builder()
            .name("id")
            .type(DataTypeInput.builder().basic(BasicType.UUID).build())
            .build());
    List<ColumnInput> values = new ArrayList<>();
    values.add(
        ColumnInput.builder()
            .name("lastname")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());
    values.add(
        ColumnInput.builder()
            .name("firstName")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());

    createTable(client, keyspace, tableName, partitionKeys, values);

    GetTableQuery.Table tableBeforeDelete = getTable(client, keyspace, tableName);
    assertThat(tableBeforeDelete.getName()).isEqualTo(tableName);

    DropTableMutation mutation =
        DropTableMutation.builder().keyspaceName(keyspace).tableName(tableName).build();

    CompletableFuture<DropTableMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<DropTableMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    DropTableMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getDropTable()).hasValue(true);

    assertThatThrownBy(
            () -> {
              getTable(client, keyspace, tableName);
            })
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("Expecting Optional to contain a value but it was empty.");
  }

  @Test
  public void alterTableAdd() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    String tableName = "tbl_altertableadd_" + System.currentTimeMillis();

    List<ColumnInput> partitionKeys = new ArrayList<>();
    partitionKeys.add(
        ColumnInput.builder()
            .name("id")
            .type(DataTypeInput.builder().basic(BasicType.UUID).build())
            .build());
    List<ColumnInput> values = new ArrayList<>();
    values.add(
        ColumnInput.builder()
            .name("lastname")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());
    values.add(
        ColumnInput.builder()
            .name("firstName")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());

    createTable(client, keyspace, tableName, partitionKeys, values);

    List<ColumnInput> columnsToAdd = new ArrayList<>();
    columnsToAdd.add(
        ColumnInput.builder()
            .name("middlename")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());

    AlterTableAddMutation mutation =
        AlterTableAddMutation.builder()
            .keyspaceName(keyspace)
            .tableName(tableName)
            .toAdd(columnsToAdd)
            .build();

    CompletableFuture<AlterTableAddMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<AlterTableAddMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    AlterTableAddMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getAlterTableAdd()).hasValue(true);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns)
        .filteredOn(c -> c.getName().equals("middlename"))
        .extracting(GetTableQuery.Column::getType)
        .anySatisfy(value -> assertThat(value.getBasic()).isEqualTo(BasicType.VARCHAR));
  }

  @Test
  public void alterTableDrop() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql-schema");
    String tableName = "tbl_altertabledrop_" + System.currentTimeMillis();

    List<ColumnInput> partitionKeys = new ArrayList<>();
    partitionKeys.add(
        ColumnInput.builder()
            .name("id")
            .type(DataTypeInput.builder().basic(BasicType.UUID).build())
            .build());
    List<ColumnInput> values = new ArrayList<>();
    values.add(
        ColumnInput.builder()
            .name("lastname")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());
    values.add(
        ColumnInput.builder()
            .name("firstName")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());
    values.add(
        ColumnInput.builder()
            .name("middlename")
            .type(DataTypeInput.builder().basic(BasicType.TEXT).build())
            .build());

    createTable(client, keyspace, tableName, partitionKeys, values);

    List<String> columnsToDrop = Collections.singletonList("middlename");

    AlterTableDropMutation mutation =
        AlterTableDropMutation.builder()
            .keyspaceName(keyspace)
            .tableName(tableName)
            .toDrop(columnsToDrop)
            .build();

    CompletableFuture<AlterTableDropMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<AlterTableDropMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    AlterTableDropMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getAlterTableDrop()).hasValue(true);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns).hasSize(3).filteredOn(c -> c.getName().equals("middlename")).isEmpty();
  }

  @Test
  public void getOrdersByValue() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    OrdersInput ordersInput = OrdersInput.builder().prodName("Medium Lift Arms").build();

    GetOrdersByValueQuery query = GetOrdersByValueQuery.builder().value(ordersInput).build();

    CompletableFuture<GetOrdersByValueQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetOrdersByValueQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetOrdersByValueQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getOrders()).isPresent();

    GetOrdersByValueQuery.Orders orders = result.getOrders().get();

    assertThat(orders.getValues()).isPresent();
    List<GetOrdersByValueQuery.Value> valuesList = orders.getValues().get();

    GetOrdersByValueQuery.Value value = valuesList.get(0);
    assertThat(value.getId()).hasValue("792d0a56-bb46-4bc2-bc41-5f4a94a83da9");
    assertThat(value.getProdId()).hasValue("31047029-2175-43ce-9fdd-b3d568b19bb2");
    assertThat(value.getProdName()).hasValue("Medium Lift Arms");
    assertThat(value.getCustomerName()).hasValue("Janice Evernathy");
    assertThat(value.getAddress()).hasValue("2101 Everplace Ave 3116");
    assertThat(value.getDescription()).hasValue("Ordering some more arms for my construction bot.");
    assertThat(value.getPrice()).hasValue((float) 3199.99);
    assertThat(value.getSellPrice()).hasValue((float) 3119.99);
  }

  @Test
  public void getOrdersWithFilter() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder()
            .prodName(StringFilterInput.builder().eq("Basic Task CPU").build())
            .customerName(StringFilterInput.builder().eq("John Doe").build())
            .build();

    QueryOptions options =
        QueryOptions.builder().consistency(QueryConsistency.LOCAL_QUORUM).build();

    GetOrdersWithFilterQuery query =
        GetOrdersWithFilterQuery.builder().filter(filterInput).options(options).build();

    CompletableFuture<GetOrdersWithFilterQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetOrdersWithFilterQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetOrdersWithFilterQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getOrders()).isPresent();

    GetOrdersWithFilterQuery.Orders orders = result.getOrders().get();

    assertThat(orders.getValues()).isPresent();
    List<GetOrdersWithFilterQuery.Value> valuesList = orders.getValues().get();

    GetOrdersWithFilterQuery.Value value = valuesList.get(0);
    assertThat(value.getId()).hasValue("dd73afe2-9841-4ce1-b841-575b8be405c1");
    assertThat(value.getProdId()).hasValue("31047029-2175-43ce-9fdd-b3d568b19bb5");
    assertThat(value.getProdName()).hasValue("Basic Task CPU");
    assertThat(value.getCustomerName()).hasValue("John Doe");
    assertThat(value.getAddress()).hasValue("123 Main St 67890");
    assertThat(value.getDescription()).hasValue("Ordering replacement CPUs.");
    assertThat(value.getPrice()).hasValue((float) 899.99);
    assertThat(value.getSellPrice()).hasValue((float) 900.82);
  }

  @Test
  public void insertProducts() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    String productId = UUID.randomUUID().toString();
    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price((float) 3199.99)
            .created(Instant.now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, input);

    GetProductsWithFilterQuery.Value product = getProduct(client, productId);

    assertThat(product.getId()).hasValue(productId);
    assertThat(product.getName()).hasValue(input.name());
    assertThat(product.getPrice()).hasValue(input.price());
    assertThat(product.getCreated()).hasValue(input.created());
    assertThat(product.getDescription()).hasValue(input.description());
  }

  public GetProductsWithFilterQuery.Value getProduct(ApolloClient client, String productId) {
    ProductsFilterInput filterInput =
        ProductsFilterInput.builder().id(UuidFilterInput.builder().eq(productId).build()).build();

    QueryOptions options =
        QueryOptions.builder().consistency(QueryConsistency.LOCAL_QUORUM).build();

    GetProductsWithFilterQuery query =
        GetProductsWithFilterQuery.builder().filter(filterInput).options(options).build();

    GetProductsWithFilterQuery.Data result = getObservable(client.query(query));
    assertThat(result.getProducts()).isPresent();
    GetProductsWithFilterQuery.Products products = result.getProducts().get();
    assertThat(products.getValues()).isPresent();
    List<GetProductsWithFilterQuery.Value> valuesList = products.getValues().get();

    return valuesList.get(0);
  }

  public InsertProductsMutation.InsertProducts insertProduct(
      ApolloClient client, ProductsInput input) {
    InsertProductsMutation mutation = InsertProductsMutation.builder().value(input).build();
    InsertProductsMutation.Data result = getObservable(client.mutate(mutation));
    assertThat(result.getInsertProducts()).isPresent();
    return result.getInsertProducts().get();
  }

  @Test
  public void updateProducts() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    String productId = UUID.randomUUID().toString();
    ProductsInput insertInput =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price((float) 3199.99)
            .created(Instant.now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, insertInput);

    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name(insertInput.name())
            .price(insertInput.price())
            .created(insertInput.created())
            .description("Normal legs but shiny. Now available in different colors")
            .build();

    UpdateProductsMutation mutation = UpdateProductsMutation.builder().value(input).build();
    UpdateProductsMutation.Data result = getObservable(client.mutate(mutation));
    assertThat(result.getUpdateProducts()).isPresent();
    GetProductsWithFilterQuery.Value product = getProduct(client, productId);

    assertThat(product.getId()).hasValue(productId);
    assertThat(product.getName()).hasValue(input.name());
    assertThat(product.getPrice()).hasValue(input.price());
    assertThat(product.getCreated()).hasValue(input.created());
    assertThat(product.getDescription()).hasValue(input.description());
  }

  @Test
  public void deleteProducts() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    String productId = UUID.randomUUID().toString();
    ProductsInput insertInput =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price((float) 3199.99)
            .created(Instant.now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, insertInput);

    DeleteProductsMutation mutation =
        DeleteProductsMutation.builder()
            .value(ProductsInput.builder().id(productId).build())
            .build();

    DeleteProductsMutation.Data result = getObservable(client.mutate(mutation));

    assertThat(result.getDeleteProducts()).isPresent();

    assertThatThrownBy(
            () -> {
              getProduct(client, productId);
            })
        .isInstanceOf(IndexOutOfBoundsException.class)
        .hasMessageContaining("Index: 0, Size: 0");
  }

  @Test
  public void invalidTypeMappingReturnsErrorResponse() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");
    // Expected UUID format
    GraphQLTestException ex =
        catchThrowableOfType(() -> getProduct(client, "zzz"), GraphQLTestException.class);
    assertThat(ex.errors).hasSize(1);
    assertThat(ex.errors.get(0).getMessage()).contains("Invalid UUID string");
  }

  @SuppressWarnings("unchecked")
  @ParameterizedTest
  @MethodSource("getInvalidQueries")
  @DisplayName("Invalid GraphQL queries and mutations should return error response")
  public void invalidGraphQLParametersReturnsErrorResponse(
      String path, String query, String message1, String message2) throws IOException {

    OkHttpClient okHttpClient =
        new OkHttpClient.Builder()
            .addInterceptor(
                chain ->
                    chain.proceed(
                        chain
                            .request()
                            .newBuilder()
                            .addHeader("X-Cassandra-Token", authToken)
                            .addHeader("content-type", "application/json")
                            .build()))
            .build();

    String url = String.format("http://%s:8080%s", getStargateHost(), path);
    HttpUrl.Builder httpBuilder = HttpUrl.parse(url).newBuilder();
    httpBuilder.addQueryParameter("query", query);
    okhttp3.Response response =
        okHttpClient.newCall(new Request.Builder().url(httpBuilder.build()).build()).execute();
    assertThat(response.code()).isEqualTo(HttpStatus.SC_OK);
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> mapResponse = mapper.readValue(response.body().string(), Map.class);
    assertThat(mapResponse).containsKey("errors");
    assertThat(mapResponse.get("errors")).isInstanceOf(List.class);
    List<Map<String, Object>> errors = (List<Map<String, Object>>) mapResponse.get("errors");
    assertThat(errors)
        .hasOnlyOneElementSatisfying(
            item -> assertThat(item.get("message")).asString().contains(message1, message2));
    response.close();
  }

  public static Stream<Arguments> getInvalidQueries() {
    String dmlPath = "/graphql/betterbotz";
    String ddlPath = "/graphql-schema";
    return Stream.of(
        arguments(
            dmlPath,
            "query { zzz { name } }",
            "Field 'zzz' in type 'Query' is undefined",
            "Validation error"),
        arguments(
            dmlPath,
            "invalidWrapper { zzz { name } }",
            "offending token 'invalidWrapper'",
            "Invalid Syntax"),
        arguments(
            dmlPath,
            "query { products(filter: { name: { gt: \"a\"} }) { values { id } }}",
            "Cannot execute this query",
            "use ALLOW FILTERING"),
        arguments(
            ddlPath,
            "query { zzz { name } }",
            "Field 'zzz' in type 'Query' is undefined",
            "Validation error"),
        arguments(
            ddlPath,
            "query { keyspace (name: 1) { name } }",
            "WrongType: argument 'name'",
            "Validation error"),
        arguments(
            ddlPath,
            "query { keyspaces { name, nameInvalid } }",
            "Field 'nameInvalid' in type 'Keyspace' is undefined",
            "Validation error"));
  }

  @Test
  public void shouldInsertAndUpdateSimpleListSetsAndMaps() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");
    UUID id = UUID.randomUUID();

    List<Integer> list = Arrays.asList(1, 2, 3);
    List<String> set = Arrays.asList("a", "b", "c");
    List<Map<Integer, String>> map =
        Arrays.asList(
            ImmutableMap.<Integer, String>builder().put(1, "one").build(),
            ImmutableMap.<Integer, String>builder().put(2, "two").build());

    InsertCollectionsSimpleMutation insertMutation =
        InsertCollectionsSimpleMutation.builder()
            .value(
                CollectionsSimpleInput.builder()
                    .id(id)
                    .listValue1(list)
                    .setValue1(set)
                    .mapValue1(
                        map.stream()
                            .map(m -> m.entrySet().stream().findFirst().get())
                            .map(
                                e ->
                                    InputKeyIntValueString.builder()
                                        .key(e.getKey())
                                        .value(e.getValue())
                                        .build())
                            .collect(Collectors.toList()))
                    .build())
            .build();

    InsertCollectionsSimpleMutation.Data insertResult = mutateAndGet(client, insertMutation);
    assertThat(insertResult.getInsertCollectionsSimple()).isPresent();
    assertCollectionsSimple(client, id, list, set, map);

    list = Arrays.asList(4, 5);
    set = Collections.singletonList("d");
    map =
        Collections.singletonList(ImmutableMap.<Integer, String>builder().put(3, "three").build());

    UpdateCollectionsSimpleMutation updateMutation =
        UpdateCollectionsSimpleMutation.builder()
            .value(
                CollectionsSimpleInput.builder()
                    .id(id)
                    .listValue1(list)
                    .setValue1(set)
                    .mapValue1(
                        map.stream()
                            .map(m -> m.entrySet().stream().findFirst().get())
                            .map(
                                e ->
                                    InputKeyIntValueString.builder()
                                        .key(e.getKey())
                                        .value(e.getValue())
                                        .build())
                            .collect(Collectors.toList()))
                    .build())
            .build();

    UpdateCollectionsSimpleMutation.Data updateResult = mutateAndGet(client, updateMutation);
    assertThat(updateResult.getUpdateCollectionsSimple()).isPresent();
    assertCollectionsSimple(client, id, list, set, map);
  }

  @Test
  @DisplayName("Should insert and read back UDTs")
  public void udtsTest() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    InsertUdtsMutation insert =
        InsertUdtsMutation.builder()
            .value(
                UdtsInput.builder()
                    .a(AUdtInput.builder().b(BUdtInput.builder().i(1).build()).build())
                    .bs(
                        ImmutableList.of(
                            BUdtInput.builder().i(2).build(), BUdtInput.builder().i(3).build()))
                    .build())
            .build();
    mutateAndGet(client, insert);

    GetUdtsQuery select =
        GetUdtsQuery.builder()
            .value(
                UdtsInput.builder()
                    .a(AUdtInput.builder().b(BUdtInput.builder().i(1).build()).build())
                    .build())
            .build();
    List<GetUdtsQuery.Value> values =
        getObservable(client.query(select))
            .getUdts()
            .flatMap(GetUdtsQuery.Udts::getValues)
            .orElseThrow(AssertionError::new);
    assertThat(values).hasSize(1);
    GetUdtsQuery.Value result = values.get(0);
    assertThat(result.getA().flatMap(GetUdtsQuery.A::getB))
        .flatMap(GetUdtsQuery.B::getI)
        .hasValue(1);
    assertThat(result.getBs())
        .hasValueSatisfying(
            bs -> {
              assertThat(bs).hasSize(2);
              assertThat(bs.get(0).getI()).hasValue(2);
              assertThat(bs.get(1).getI()).hasValue(3);
            });
  }

  private void assertCollectionsSimple(
      ApolloClient client,
      UUID id,
      List<Integer> list,
      List<String> set,
      List<Map<Integer, String>> map) {
    GetCollectionsSimpleQuery.Value item = getCollectionSimple(client, id);
    assertThat(item.getListValue1()).isPresent();
    assertThat(item.getListValue1().get()).isEqualTo(list);
    assertThat(item.getSetValue1()).isPresent();
    assertThat(item.getSetValue1().get()).isEqualTo(set);
    assertThat(item.getMapValue1()).isPresent();
    assertThat(
            item.getMapValue1().get().stream()
                .map(
                    m ->
                        ImmutableMap.<Integer, String>builder()
                            .put(m.getKey(), m.getValue().get())
                            .build())
                .collect(Collectors.toList()))
        .isEqualTo(map);
  }

  private GetCollectionsSimpleQuery.Value getCollectionSimple(ApolloClient client, UUID id) {
    GetCollectionsSimpleQuery getQuery =
        GetCollectionsSimpleQuery.builder()
            .value(CollectionsSimpleInput.builder().id(id).build())
            .build();

    GetCollectionsSimpleQuery.Data getResult = getObservable(client.query(getQuery));
    assertThat(getResult.getCollectionsSimple()).isPresent();
    assertThat(getResult.getCollectionsSimple().get().getValues()).isPresent();
    assertThat(getResult.getCollectionsSimple().get().getValues().get()).hasSize(1);
    return getResult.getCollectionsSimple().get().getValues().get().get(0);
  }

  private static <T> T getObservable(ApolloCall<Optional<T>> observable) {
    CompletableFuture<T> future = new CompletableFuture<>();
    observable.enqueue(queryCallback(future));

    try {
      return future.get();
    } catch (ExecutionException e) {
      // Unwrap exception
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException("Unexpected exception", e);
    } catch (Exception e) {
      throw new RuntimeException("Operation could not be completed", e);
    } finally {
      observable.cancel();
    }
  }

  @SuppressWarnings("unchecked")
  private static <D extends Operation.Data, T, V extends Operation.Variables> D mutateAndGet(
      ApolloClient client, Mutation<D, T, V> mutation) {
    return getObservable((ApolloMutationCall<Optional<D>>) client.mutate(mutation));
  }

  private ApolloClient getApolloClient(String path) {
    OkHttpClient okHttpClient =
        new OkHttpClient.Builder()
            .addInterceptor(
                chain ->
                    chain.proceed(
                        chain
                            .request()
                            .newBuilder()
                            .addHeader("X-Cassandra-Token", authToken)
                            .build()))
            .build();

    return ApolloClient.builder()
        .serverUrl(String.format("http://%s:8080%s", getStargateHost(), path))
        .okHttpClient(okHttpClient)
        .addCustomTypeAdapter(
            CustomType.TIMESTAMP,
            new CustomTypeAdapter<Instant>() {
              @NotNull
              @Override
              public CustomTypeValue<?> encode(Instant instant) {
                return new CustomTypeValue.GraphQLString(instant.toString());
              }

              @Override
              public Instant decode(@NotNull CustomTypeValue<?> customTypeValue) {
                return Instant.parse(customTypeValue.value.toString());
              }
            })
        .build();
  }

  private static <U> ApolloCall.Callback<Optional<U>> queryCallback(CompletableFuture<U> future) {
    return new ApolloCall.Callback<Optional<U>>() {
      @Override
      public void onResponse(@NotNull Response<Optional<U>> response) {
        if (response.getData().isPresent()) {
          future.complete(response.getData().get());
          return;
        }

        if (response.getErrors() != null && response.getErrors().size() > 0) {
          future.completeExceptionally(
              new GraphQLTestException("GraphQL error response", response.getErrors()));
          return;
        }

        future.completeExceptionally(
            new IllegalStateException("Unexpected empty data and errors properties"));
      }

      @Override
      public void onFailure(@NotNull ApolloException e) {
        future.completeExceptionally(e);
      }
    };
  }

  private static class GraphQLTestException extends RuntimeException {
    private final List<Error> errors;

    GraphQLTestException(String message, List<Error> errors) {
      super(message);
      this.errors = errors;
    }
  }
}
