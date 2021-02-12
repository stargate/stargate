package io.stargate.it.http;

import static io.stargate.it.MetricsTestsHelper.getMetricValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloMutationCall;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.Error;
import com.apollographql.apollo.api.Mutation;
import com.apollographql.apollo.api.Operation;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.example.graphql.client.betterbotz.atomic.InsertOrdersWithAtomicMutation;
import com.example.graphql.client.betterbotz.atomic.ProductsAndOrdersMutation;
import com.example.graphql.client.betterbotz.collections.GetCollectionsNestedQuery;
import com.example.graphql.client.betterbotz.collections.GetCollectionsSimpleQuery;
import com.example.graphql.client.betterbotz.collections.InsertCollectionsNestedMutation;
import com.example.graphql.client.betterbotz.collections.InsertCollectionsSimpleMutation;
import com.example.graphql.client.betterbotz.collections.UpdateCollectionsSimpleMutation;
import com.example.graphql.client.betterbotz.orders.GetOrdersByValueQuery;
import com.example.graphql.client.betterbotz.orders.GetOrdersWithFilterQuery;
import com.example.graphql.client.betterbotz.products.DeleteProductsMutation;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery.Products;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery.Value;
import com.example.graphql.client.betterbotz.products.InsertProductsMutation;
import com.example.graphql.client.betterbotz.products.UpdateProductsMutation;
import com.example.graphql.client.betterbotz.tuples.GetTuplesPkQuery;
import com.example.graphql.client.betterbotz.tuples.GetTuplesQuery;
import com.example.graphql.client.betterbotz.tuples.InsertTuplesMutation;
import com.example.graphql.client.betterbotz.tuples.InsertTuplesPkMutation;
import com.example.graphql.client.betterbotz.tuples.UpdateTuplesMutation;
import com.example.graphql.client.betterbotz.type.AUdtInput;
import com.example.graphql.client.betterbotz.type.BUdtInput;
import com.example.graphql.client.betterbotz.type.CollectionsNestedInput;
import com.example.graphql.client.betterbotz.type.CollectionsSimpleInput;
import com.example.graphql.client.betterbotz.type.EntryBigIntKeyStringValueInput;
import com.example.graphql.client.betterbotz.type.EntryIntKeyStringValueInput;
import com.example.graphql.client.betterbotz.type.EntryUuidKeyListEntryBigIntKeyStringValueInputValueInput;
import com.example.graphql.client.betterbotz.type.MutationConsistency;
import com.example.graphql.client.betterbotz.type.MutationOptions;
import com.example.graphql.client.betterbotz.type.OrdersFilterInput;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.ProductsFilterInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import com.example.graphql.client.betterbotz.type.QueryConsistency;
import com.example.graphql.client.betterbotz.type.QueryOptions;
import com.example.graphql.client.betterbotz.type.StringFilterInput;
import com.example.graphql.client.betterbotz.type.TupleIntIntInput;
import com.example.graphql.client.betterbotz.type.Tuplx65_sPkInput;
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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.stargate.db.schema.Column;
import io.stargate.it.http.graphql.GraphqlITBase;
import io.stargate.it.http.graphql.TupleHelper;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.http.HttpStatus;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
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
public class GraphqlTest extends GraphqlITBase {
  private static final Pattern GRAPHQL_OPERATIONS_METRIC_REGEXP =
      Pattern.compile(
          "(graphqlapi_io_dropwizard_jetty_MutableServletContextHandler_dispatches_count\\s*)(\\d+.\\d+)");

  private static final String keyspace = "betterbotz";

  @BeforeAll
  public static void setup() throws Exception {
    createSchema();
  }

  private static void createSchema() throws Exception {
    // Create CQL schema using betterbotz.cql file
    InputStream inputStream =
        GraphqlTest.class.getClassLoader().getResourceAsStream("betterbotz.cql");
    assertThat(inputStream).isNotNull();
    String queries = CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
    assertThat(queries).isNotNull();

    for (String q : Splitter.on(';').split(queries)) {
      if (q.trim().equals("")) {
        continue;
      }
      session.execute(q);
    }

    PreparedStatement insert =
        session.prepare(
            String.format(
                "insert into %s.\"Orders\" (id, \"prodId\", \"prodName\", description, price,"
                    + "\"sellPrice\", \"customerName\", address) values (?, ?, ?, ?, ?, ?, ?, ?)",
                keyspace));

    session.execute(
        insert.bind(
            UUID.fromString("792d0a56-bb46-4bc2-bc41-5f4a94a83da9"),
            UUID.fromString("31047029-2175-43ce-9fdd-b3d568b19bb2"),
            "Medium Lift Arms",
            "Ordering some more arms for my construction bot.",
            BigDecimal.valueOf(3199.99),
            BigDecimal.valueOf(3119.99),
            "Janice Evernathy",
            "2101 Everplace Ave 3116"));

    session.execute(
        insert.bind(
            UUID.fromString("dd73afe2-9841-4ce1-b841-575b8be405c1"),
            UUID.fromString("31047029-2175-43ce-9fdd-b3d568b19bb5"),
            "Basic Task CPU",
            "Ordering replacement CPUs.",
            BigDecimal.valueOf(899.99),
            BigDecimal.valueOf(900.82),
            "John Doe",
            "123 Main St 67890"));
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
                        "create table %s.test (id uuid, primary key (id))", newKeyspaceName))
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

    createTable(client, tableName, builder, keyspace);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns).filteredOn(c -> c.getName().equals("pk1")).hasSize(1);
    assertThat(columns)
        .filteredOn(c -> c.getName().equals("ck1") || c.getName().equals("ck2"))
        .hasSize(2);
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
    assertThat(value.getPrice()).hasValue("3199.99");
    assertThat(value.getSellPrice()).hasValue("3119.99");
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
    assertThat(value.getPrice()).hasValue("899.99");
    assertThat(value.getSellPrice()).hasValue("900.82");
  }

  @Test
  public void insertProducts() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    String productId = UUID.randomUUID().toString();
    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
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
    List<GetProductsWithFilterQuery.Value> valuesList = getProductValues(client, productId);
    return valuesList.get(0);
  }

  public List<Value> getProductValues(ApolloClient client, String productId) {
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
    return products.getValues().get();
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
            .price("3199.99")
            .created(now())
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
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, insertInput);

    DeleteProductsMutation mutation =
        DeleteProductsMutation.builder()
            .value(ProductsInput.builder().id(productId).build())
            .build();

    DeleteProductsMutation.Data result = getObservable(client.mutate(mutation));

    assertThat(result.getDeleteProducts()).isPresent();

    assertThat(getProductValues(client, productId)).hasSize(0);
  }

  @Test
  @DisplayName("Should execute multiple mutations with atomic directive")
  public void shouldSupportMultipleMutationsWithAtomicDirective() {
    UUID id = UUID.randomUUID();
    String productName = "prod " + id;
    String customer = "cust " + id;
    String price = "123";
    String description = "desc " + id;

    ApolloClient client = getApolloClient("/graphql/betterbotz");
    ProductsAndOrdersMutation mutation =
        ProductsAndOrdersMutation.builder()
            .productValue(
                ProductsInput.builder()
                    .id(id.toString())
                    .prodName(productName)
                    .price(price)
                    .name(productName)
                    .customerName(customer)
                    .created(now())
                    .description(description)
                    .build())
            .orderValue(
                OrdersInput.builder()
                    .prodName(productName)
                    .customerName(customer)
                    .price(price)
                    .description(description)
                    .build())
            .build();

    getObservable(client.mutate(mutation));

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM betterbotz.\"Products\" WHERE id = ?", id))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"prodName\""), r -> r.getString("description"))
        .containsExactly(productName, description);

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM betterbotz.\"Orders\" WHERE \"prodName\" = ?", productName))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"customerName\""), r -> r.getString("description"))
        .containsExactly(customer, description);
  }

  @Test
  @DisplayName("Should execute single mutation with atomic directive")
  public void shouldSupportSingleMutationWithAtomicDirective() {
    UUID id = UUID.randomUUID();
    String productName = "prod " + id;
    String description = "desc " + id;
    String customer = "cust 1";

    ApolloClient client = getApolloClient("/graphql/betterbotz");
    InsertOrdersWithAtomicMutation mutation =
        InsertOrdersWithAtomicMutation.builder()
            .value(
                OrdersInput.builder()
                    .prodName(productName)
                    .customerName(customer)
                    .price("456")
                    .description(description)
                    .build())
            .build();

    getObservable(client.mutate(mutation));

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM betterbotz.\"Orders\" WHERE \"prodName\" = ?", productName))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"customerName\""), r -> r.getString("description"))
        .containsExactly(customer, description);
  }

  @Test
  @DisplayName(
      "When invalid, multiple mutations with atomic directive should return error response")
  public void multipleMutationsWithAtomicDirectiveShouldReturnErrorResponse() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");
    ProductsAndOrdersMutation mutation =
        ProductsAndOrdersMutation.builder()
            .productValue(
                // The mutation is invalid as parts of the primary key are missing
                ProductsInput.builder()
                    .id(UUID.randomUUID().toString())
                    .prodName("prodName sample")
                    .customerName("customer name")
                    .build())
            .orderValue(
                OrdersInput.builder().prodName("a").customerName("b").description("c").build())
            .build();

    GraphQLTestException ex =
        catchThrowableOfType(
            () -> getObservable(client.mutate(mutation)), GraphQLTestException.class);

    assertThat(ex).isNotNull();
    assertThat(ex.errors)
        // One error per query
        .hasSize(2)
        .first()
        .extracting(Error::getMessage)
        .asString()
        .contains("Some clustering keys are missing");
  }

  @Test
  @DisplayName("Multiple options with atomic directive should return error response")
  public void multipleOptionsWithAtomicDirectiveShouldReturnErrorResponse() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    ProductsAndOrdersMutation mutation =
        ProductsAndOrdersMutation.builder()
            .productValue(
                ProductsInput.builder()
                    .id(Uuids.random().toString())
                    .prodName("prod 1")
                    .price("1")
                    .name("prod1")
                    .created(now())
                    .build())
            .orderValue(
                OrdersInput.builder()
                    .prodName("prod 1")
                    .customerName("cust 1")
                    .description("my description")
                    .build())
            .options(MutationOptions.builder().consistency(MutationConsistency.ALL).build())
            .build();

    GraphQLTestException ex =
        catchThrowableOfType(
            () -> getObservable(client.mutate(mutation)), GraphQLTestException.class);

    assertThat(ex).isNotNull();
    assertThat(ex.errors)
        // One error per query
        .hasSize(2)
        .first()
        .extracting(Error::getMessage)
        .asString()
        .contains("options can only de defined once in an @atomic mutation selection");
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
  private Map<String, Object> executePost(String path, String query) throws IOException {
    OkHttpClient okHttpClient = getHttpClient();
    String url = String.format("http://%s:8080%s", stargate.seedAddress(), path);
    Map<String, Object> formData = new HashMap<>();
    formData.put("query", query);

    MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    okhttp3.Response response =
        okHttpClient
            .newCall(
                new Request.Builder()
                    .post(RequestBody.create(JSON, objectMapper.writeValueAsBytes(formData)))
                    .url(url)
                    .build())
            .execute();
    assertThat(response.code()).isEqualTo(HttpStatus.SC_OK);
    Map<String, Object> result = objectMapper.readValue(response.body().string(), Map.class);
    response.close();
    return result;
  }

  @ParameterizedTest
  @MethodSource("getInvalidQueries")
  @DisplayName("Invalid GraphQL queries and mutations should return error response")
  public void invalidGraphQLParametersReturnsErrorResponse(
      String path, String query, String message1, String message2) throws IOException {
    Map<String, Object> mapResponse = executePost(path, query);
    assertThat(mapResponse).containsKey("errors");
    assertThat(mapResponse.get("errors")).isInstanceOf(List.class);
    List<Map<String, Object>> errors = (List<Map<String, Object>>) mapResponse.get("errors");
    assertThat(errors)
        .hasSize(1)
        .first()
        .extracting(i -> i.get("message"))
        .asString()
        .contains(message1, message2);
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
            "query { Products(filter: { name: { gt: \"a\"} }) { values { id } }}",
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

  @ParameterizedTest
  @MethodSource("getScalarValues")
  public void shouldSupportScalar(Column.Type type, Object value) throws IOException {
    String column = type.name().toLowerCase() + "value";
    UUID id = UUID.randomUUID();
    String mutation = "mutation { updateScalars(value: {id: \"%s\", %s: %s}) { applied } }";

    String graphQLValue = value.toString();
    if (value instanceof String) {
      graphQLValue = String.format("\"%s\"", value);
    }

    assertThat(
            executePost("/graphql/betterbotz", String.format(mutation, id, column, graphQLValue)))
        .doesNotContainKey("errors");

    String query = "query { Scalars(value: {id: \"%s\"}) { values { %s } } }";
    Map<String, Object> result =
        executePost("/graphql/betterbotz", String.format(query, id, column));

    assertThat(result).doesNotContainKey("errors");
    assertThat(result)
        .extractingByKey("data", InstanceOfAssertFactories.MAP)
        .extractingByKey("Scalars", InstanceOfAssertFactories.MAP)
        .extractingByKey("values", InstanceOfAssertFactories.LIST)
        .singleElement()
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .extractingByKey(column)
        .isEqualTo(value);
  }

  @SuppressWarnings("unused") // referenced by @MethodSource
  private static Stream<Arguments> getScalarValues() {
    return Stream.of(
        arguments(Column.Type.Ascii, "abc"),
        arguments(Column.Type.Bigint, "-9223372036854775807"),
        arguments(Column.Type.Blob, "AQID//7gEiMB"),
        arguments(Column.Type.Boolean, true),
        arguments(Column.Type.Boolean, false),
        arguments(Column.Type.Date, "2005-08-05"),
        arguments(Column.Type.Decimal, "-0.123456"),
        arguments(Column.Type.Double, -1D),
        arguments(Column.Type.Duration, "12h30m"),
        // Serialized as JSON numbers
        arguments(Column.Type.Float, 1.1234D),
        arguments(Column.Type.Inet, "8.8.8.8"),
        arguments(Column.Type.Int, 1),
        // Serialized as JSON Number
        arguments(Column.Type.Smallint, 32_767),
        arguments(Column.Type.Text, "abc123", "'abc123'"),
        arguments(Column.Type.Time, "23:59:31.123456789"),
        arguments(Column.Type.Timestamp, formatInstant(now())),
        arguments(Column.Type.Tinyint, -128),
        arguments(Column.Type.Tinyint, 1),
        arguments(Column.Type.Timeuuid, Uuids.timeBased().toString()),
        arguments(Column.Type.Uuid, "f3abdfbf-479f-407b-9fde-128145bd7bef"),
        arguments(Column.Type.Varchar, ""),
        arguments(Column.Type.Varint, "92233720368547758070000"));
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
                    .mapValue1(toInputKeyIntValueStringList(map))
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
                    .mapValue1(toInputKeyIntValueStringList(map))
                    .build())
            .build();

    UpdateCollectionsSimpleMutation.Data updateResult = mutateAndGet(client, updateMutation);
    assertThat(updateResult.getUpdateCollectionsSimple()).isPresent();
    assertCollectionsSimple(client, id, list, set, map);
  }

  @Test
  public void shouldInsertAndUpdateNestedListSetsAndMaps() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");
    UUID id = UUID.randomUUID();

    List<List<EntryIntKeyStringValueInput>> list =
        Collections.singletonList(
            toInputKeyIntValueStringList(
                Collections.singletonList(
                    ImmutableMap.<Integer, String>builder().put(3, "three").build())));
    List<List<Object>> set =
        Collections.singletonList(
            ImmutableList.builder()
                .add(Uuids.timeBased().toString(), Uuids.timeBased().toString())
                .build());

    List<EntryUuidKeyListEntryBigIntKeyStringValueInputValueInput> map =
        Collections.singletonList(
            EntryUuidKeyListEntryBigIntKeyStringValueInputValueInput.builder()
                .key(Uuids.random().toString())
                .value(
                    Collections.singletonList(
                        EntryBigIntKeyStringValueInput.builder()
                            .key("123")
                            .value("one-two-three")
                            .build()))
                .build());

    InsertCollectionsNestedMutation insertMutation =
        InsertCollectionsNestedMutation.builder()
            .value(
                CollectionsNestedInput.builder()
                    .id(id)
                    .listValue1(list)
                    .setValue1(set)
                    .mapValue1(map)
                    .build())
            .build();

    InsertCollectionsNestedMutation.Data insertResult =
        getObservable(client.mutate(insertMutation));
    assertThat(insertResult.getInsertCollectionsNested()).isPresent();
    assertCollectionsNested(client, id, list, set, map);
  }

  @Test
  public void shouldInsertAndUpdateTuples() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");
    UUID id = UUID.randomUUID();
    long tuple1Value = 1L;
    float[] tuple2 = {1.3f, -90f};
    Object[] tuple3 = {Uuids.timeBased(), 2, true};

    getObservable(
        client.mutate(
            InsertTuplesMutation.builder()
                .value(TupleHelper.createTupleInput(id, tuple1Value, tuple2, tuple3))
                .build()));

    TupleHelper.assertTuples(
        getObservable(client.query(GetTuplesQuery.builder().id(id).build())),
        tuple1Value,
        tuple2,
        tuple3);

    tuple1Value = -1L;
    tuple2 = new float[] {0, Float.MAX_VALUE};
    tuple3 = new Object[] {Uuids.timeBased(), 3, false};

    getObservable(
        client.mutate(
            UpdateTuplesMutation.builder()
                .value(TupleHelper.createTupleInput(id, tuple1Value, tuple2, tuple3))
                .build()));

    TupleHelper.assertTuples(
        getObservable(client.query(GetTuplesQuery.builder().id(id).build())),
        tuple1Value,
        tuple2,
        tuple3);
  }

  @Test
  public void shouldSupportTuplesAsPartitionKey() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");
    Tuplx65_sPkInput input =
        Tuplx65_sPkInput.builder()
            .id(TupleIntIntInput.builder().item0(10).item1(20).build())
            .build();
    getObservable(client.mutate(InsertTuplesPkMutation.builder().value(input).build()));

    GetTuplesPkQuery.Data result =
        getObservable(client.query(GetTuplesPkQuery.builder().value(input).build()));

    assertThat(result.getTuplx65_sPk())
        .isPresent()
        .get()
        .extracting(v -> v.getValues(), InstanceOfAssertFactories.OPTIONAL)
        .isPresent()
        .get(InstanceOfAssertFactories.LIST)
        .hasSize(1);
  }

  @Test
  public void queryWithPaging() {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    for (String name : ImmutableList.of("a", "b", "c")) {
      insertProduct(
          client,
          ProductsInput.builder()
              .id(UUID.randomUUID().toString())
              .name(name)
              .price("1.0")
              .created(now())
              .build());
    }

    List<String> names = new ArrayList<>();

    Optional<Products> products = Optional.empty();
    do {
      products = getProducts(client, 1, products.flatMap(r -> r.getPageState()));
      products.ifPresent(
          p -> {
            p.getValues()
                .ifPresent(
                    values -> {
                      for (Value value : values) {
                        value.getName().ifPresent(names::add);
                      }
                    });
          });
    } while (products
        .map(p -> p.getValues().map(v -> !v.isEmpty()).orElse(false))
        .orElse(false)); // Continue if there are still values

    assertThat(names).containsExactlyInAnyOrder("a", "b", "c");
  }

  @Test
  public void shouldIncrementMetricWhenExecutingGraphQlQuery() throws IOException {
    ApolloClient client = getApolloClient("/graphql/betterbotz");

    String productId = UUID.randomUUID().toString();
    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, input);

    GetProductsWithFilterQuery.Value product = getProduct(client, productId);
    assertThat(product.getId()).hasValue(productId);

    // when
    String body = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    // then
    double numberOfGraphQlOperations = getGraphQlOperations(body);
    assertThat(numberOfGraphQlOperations).isGreaterThan(0);
  }

  private double getGraphQlOperations(String body) {
    return getMetricValue(body, "graphqlapi", GRAPHQL_OPERATIONS_METRIC_REGEXP);
  }

  private static Optional<Products> getProducts(
      ApolloClient client, int pageSize, Optional<String> pageState) {
    ProductsFilterInput filterInput = ProductsFilterInput.builder().build();

    QueryOptions.Builder optionsBuilder =
        QueryOptions.builder().pageSize(pageSize).consistency(QueryConsistency.LOCAL_QUORUM);

    pageState.ifPresent(optionsBuilder::pageState);
    QueryOptions options = optionsBuilder.build();

    GetProductsWithFilterQuery query =
        GetProductsWithFilterQuery.builder().filter(filterInput).options(options).build();

    GetProductsWithFilterQuery.Data result = getObservable(client.query(query));

    assertThat(result.getProducts())
        .hasValueSatisfying(
            products -> {
              assertThat(products.getValues())
                  .hasValueSatisfying(
                      values -> {
                        assertThat(values).hasSizeLessThanOrEqualTo(pageSize);
                      });
            });

    return result.getProducts();
  }

  private static List<EntryIntKeyStringValueInput> toInputKeyIntValueStringList(
      List<Map<Integer, String>> map) {
    return map.stream()
        .map(m -> m.entrySet().stream().findFirst().get())
        .map(e -> EntryIntKeyStringValueInput.builder().key(e.getKey()).value(e.getValue()).build())
        .collect(Collectors.toList());
  }

  private void assertCollectionsNested(
      ApolloClient client,
      UUID id,
      List<List<EntryIntKeyStringValueInput>> list,
      List<List<Object>> set,
      List<EntryUuidKeyListEntryBigIntKeyStringValueInputValueInput> map) {
    GetCollectionsNestedQuery getQuery =
        GetCollectionsNestedQuery.builder()
            .value(CollectionsNestedInput.builder().id(id).build())
            .build();
    GetCollectionsNestedQuery.Data result = getObservable(client.query(getQuery));
    assertThat(result.getCollectionsNested()).isPresent();
    assertThat(result.getCollectionsNested().get().getValues()).isPresent();
    assertThat(result.getCollectionsNested().get().getValues().get()).hasSize(1);
    GetCollectionsNestedQuery.Value item =
        result.getCollectionsNested().get().getValues().get().get(0);

    // Assert list
    assertThat(item.getListValue1()).isPresent();
    assertThat(item.getListValue1().get()).hasSize(list.size());
    EntryIntKeyStringValueInput expectedListItem = list.get(0).get(0);
    assertThat(item.getListValue1().get().get(0))
        .hasSize(list.get(0).size())
        .first()
        .extracting(i -> i.getKey(), i -> i.getValue().get())
        .containsExactly(expectedListItem.key(), expectedListItem.value());

    // Assert set
    assertThat(item.getSetValue1()).isPresent();
    assertThat(item.getSetValue1().get()).isEqualTo(set);

    // Assert map
    assertThat(item.getMapValue1()).isPresent();
    assertThat(item.getMapValue1().get()).hasSize(map.size());

    EntryUuidKeyListEntryBigIntKeyStringValueInputValueInput expectedMapItem = map.get(0);
    GetCollectionsNestedQuery.MapValue1 actualMapItem = item.getMapValue1().get().get(0);
    assertThat(actualMapItem.getKey()).isEqualTo(expectedMapItem.key());
    EntryBigIntKeyStringValueInput expectedMapValue = expectedMapItem.value().get(0);
    assertThat(actualMapItem.getValue().get())
        .hasSize(1)
        .first()
        .extracting(i -> i.getKey(), i -> i.getValue().get())
        .containsExactly(expectedMapValue.key(), expectedMapValue.value());
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
}
