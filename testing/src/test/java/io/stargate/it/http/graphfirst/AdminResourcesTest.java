package io.stargate.it.http.graphfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.apollographql.apollo.ApolloClient;
import com.example.graphql.client.schema.GetTableQuery;
import com.example.graphql.client.schema.type.BasicType;
import com.example.graphql.client.schema.type.ColumnInput;
import com.example.graphql.client.schema.type.DataTypeInput;
import io.stargate.it.http.graphql.GraphqlITBase;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AdminResourcesTest extends GraphqlITBase {
  private static String KEYSPACE;

  @BeforeAll
  public static void crateTestKeyspace() {
    KEYSPACE = UUID.randomUUID().toString();
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS ks_1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };");
  }

  @Test
  public void createTable() throws ExecutionException, InterruptedException {
    // todo should we be able to craete table without any cql?
    ApolloClient client = getApolloClient("/graphqlv2/admin");
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

    createTable(client, KEYSPACE, tableName, partitionKeys, values);

    GetTableQuery.Table table = getTable(client, KEYSPACE, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    assertThat(table.getColumns()).isPresent();
    List<GetTableQuery.Column> columns = table.getColumns().get();
    assertThat(columns)
        .filteredOn(c -> c.getName().equals("id"))
        .extracting(GetTableQuery.Column::getType)
        .anySatisfy(value -> assertThat(value.getBasic()).isEqualTo(BasicType.UUID));
  }
}
