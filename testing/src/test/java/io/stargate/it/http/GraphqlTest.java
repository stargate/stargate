package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloMutationCall;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.CustomTypeAdapter;
import com.apollographql.apollo.api.CustomTypeValue;
import com.apollographql.apollo.exception.ApolloException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.example.graphql.client.betterbotz.orders.GetOrdersByValueQuery;
import com.example.graphql.client.betterbotz.orders.GetOrdersWithFilterQuery;
import com.example.graphql.client.betterbotz.products.DeleteProductsMutation;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery;
import com.example.graphql.client.betterbotz.products.InsertProductsMutation;
import com.example.graphql.client.betterbotz.products.UpdateProductsMutation;
import com.example.graphql.client.betterbotz.type.CustomType;
import com.example.graphql.client.betterbotz.type.OrdersFilterInput;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.ProductsFilterInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import com.example.graphql.client.betterbotz.type.QueryConsistency;
import com.example.graphql.client.betterbotz.type.QueryOptions;
import com.example.graphql.client.betterbotz.type.StringFilterInput;
import com.example.graphql.client.betterbotz.type.UuidFilterInput;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.OkHttpClient;
import org.apache.http.HttpStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * To update these tests:
 *
 * <ul>
 *   <li>If the schema has changed, update the `schema.json` files in `src/main/graphql`. You can
 *       use the query in `src/main/resources/introspection.graphql` (paste it into the graphql
 *       playground at ${STARGATE_HOST}:8080/playground).
 *   <li>If there are new operations, create corresponding descriptors in
 *       `src/main/graphql/betterbotz` or `src/main/graphql/schema`.
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
  public void insertProducts() throws ExecutionException, InterruptedException {
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

  public GetProductsWithFilterQuery.Value getProduct(ApolloClient client, String productId)
      throws ExecutionException, InterruptedException {
    ProductsFilterInput filterInput =
        ProductsFilterInput.builder().id(UuidFilterInput.builder().eq(productId).build()).build();

    QueryOptions options =
        QueryOptions.builder().consistency(QueryConsistency.LOCAL_QUORUM).build();

    GetProductsWithFilterQuery query =
        GetProductsWithFilterQuery.builder().filter(filterInput).options(options).build();

    CompletableFuture<GetProductsWithFilterQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetProductsWithFilterQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetProductsWithFilterQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getProducts()).isPresent();

    GetProductsWithFilterQuery.Products products = result.getProducts().get();

    assertThat(products.getValues()).isPresent();
    List<GetProductsWithFilterQuery.Value> valuesList = products.getValues().get();

    return valuesList.get(0);
  }

  public InsertProductsMutation.InsertProducts insertProduct(
      ApolloClient client, ProductsInput input) throws ExecutionException, InterruptedException {
    InsertProductsMutation mutation = InsertProductsMutation.builder().value(input).build();

    CompletableFuture<InsertProductsMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<InsertProductsMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    InsertProductsMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getInsertProducts()).isPresent();

    return result.getInsertProducts().get();
  }

  @Test
  public void updateProducts() throws ExecutionException, InterruptedException {
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

    CompletableFuture<UpdateProductsMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<UpdateProductsMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    UpdateProductsMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getUpdateProducts()).isPresent();

    GetProductsWithFilterQuery.Value product = getProduct(client, productId);

    assertThat(product.getId()).hasValue(productId);
    assertThat(product.getName()).hasValue(input.name());
    assertThat(product.getPrice()).hasValue(input.price());
    assertThat(product.getCreated()).hasValue(input.created());
    assertThat(product.getDescription()).hasValue(input.description());
  }

  @Test
  public void deleteProducts() throws ExecutionException, InterruptedException {
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

    CompletableFuture<DeleteProductsMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<DeleteProductsMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    DeleteProductsMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getDeleteProducts()).isPresent();

    assertThatThrownBy(
            () -> {
              getProduct(client, productId);
            })
        .isInstanceOf(IndexOutOfBoundsException.class)
        .hasMessageContaining("Index: 0, Size: 0");
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
      public void onResponse(@NotNull com.apollographql.apollo.api.Response<Optional<U>> response) {
        if (response.getData().isPresent()) {
          future.complete(response.getData().get());
        } else {
          future.complete(null);
        }
      }

      @Override
      public void onFailure(@NotNull ApolloException e) {
        future.completeExceptionally(e);
      }
    };
  }
}
