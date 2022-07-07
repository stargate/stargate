package io.stargate.db.datastore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import io.stargate.db.ImmutableParameters;
import io.stargate.db.Persistence;
import io.stargate.db.Persistence.Connection;
import io.stargate.db.Result;
import io.stargate.db.Result.ResultMetadata;
import io.stargate.db.Result.Rows;
import io.stargate.db.SimpleStatement;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableKeyspace;
import io.stargate.db.schema.ImmutableSchema;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PersistenceBackedResultSetTest {
  private Connection connection;
  private Rows rowsSameUser;
  private Rows rowsDifferentUser;

  @BeforeEach
  void setup() {
    connection = mock(Connection.class);
    Schema schema =
        ImmutableSchema.builder()
            .addKeyspaces(
                ImmutableKeyspace.builder().name("store").addTables(SHOPPING_CART).build())
            .build();

    Persistence persistence = mock(Persistence.class);
    when(connection.persistence()).thenReturn(persistence);
    when(persistence.schema()).thenReturn(schema);

    List<Map<String, Object>> rowValsSameUser = new ArrayList<>();
    Map<String, Object> rowVal = new HashMap<>();
    rowVal.put("userid", "123");
    rowVal.put("item_count", 2);
    rowVal.put("last_update_timestamp", Instant.now());
    rowValsSameUser.add(rowVal);

    rowVal = new HashMap<>();
    rowVal.put("userid", "123");
    rowVal.put("item_count", 5);
    rowVal.put("last_update_timestamp", Instant.now());
    rowValsSameUser.add(rowVal);
    rowsSameUser = createRows(SHOPPING_CART.columns(), rowValsSameUser);

    List<Map<String, Object>> rowValsDifferentUser = new ArrayList<>();
    Map<String, Object> rowValDifferent = new HashMap<>();
    rowValDifferent.put("userid", "123");
    rowValDifferent.put("item_count", 2);
    rowValDifferent.put("last_update_timestamp", Instant.now());
    rowValsDifferentUser.add(rowValDifferent);

    rowValDifferent = new HashMap<>();
    rowValDifferent.put("userid", "456");
    rowValDifferent.put("item_count", 10);
    rowValDifferent.put("last_update_timestamp", Instant.now());
    rowValsDifferentUser.add(rowValDifferent);
    rowsDifferentUser = createRows(SHOPPING_CART.columns(), rowValsDifferentUser);
  }

  @Test
  public void one() {
    Map<String, Object> data = new HashMap<>();
    data.put("userid", "123");
    data.put("item_count", 2);
    data.put("last_update_timestamp", Instant.now());

    Rows rows = createRows(SHOPPING_CART.columns(), Collections.singletonList(data));

    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rows);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Row one = resultSet.one();

    assertThat(one).isNotNull();
    assertRow(one, rows.rows.get(0));
  }

  @Test
  public void oneNotAuthorized() {
    Map<String, Object> data = new HashMap<>();
    data.put("userid", "123");
    data.put("item_count", 2);
    data.put("last_update_timestamp", Instant.now());

    Rows rows = createRows(SHOPPING_CART.columns(), Collections.singletonList(data));

    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "456");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rows);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    assertThrows(NoSuchElementException.class, resultSet::one);
  }

  @Test
  public void oneNoClaims() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Row one = resultSet.one();

    assertThat(one).isNotNull();
    assertRow(one, rowsSameUser.rows.get(0));
  }

  @Test
  public void oneMultipleRows() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Row one = resultSet.one();

    assertThat(one).isNotNull();
    assertRow(one, rowsSameUser.rows.get(0));
  }

  @Test
  public void oneMultipleRowsDifferentUserIds() {
    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> rowVal = new HashMap<>();
    rowVal.put("userid", "123");
    rowVal.put("item_count", 2);
    rowVal.put("last_update_timestamp", Instant.now());
    data.add(rowVal);

    rowVal = new HashMap<>();
    rowVal.put("userid", "456");
    rowVal.put("item_count", 5);
    rowVal.put("last_update_timestamp", Instant.now());
    data.add(rowVal);

    Rows rows = createRows(SHOPPING_CART.columns(), data);

    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rows);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Row one = resultSet.one();

    assertThat(one).isNotNull();
    assertRow(one, rows.rows.get(0));
  }

  @Test
  public void oneMultipleRowsNoFilter() {
    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> rowVal = new HashMap<>();
    rowVal.put("userid", "123");
    rowVal.put("item_count", 2);
    rowVal.put("last_update_timestamp", Instant.now());
    data.add(rowVal);

    rowVal = new HashMap<>();
    rowVal.put("userid", "456");
    rowVal.put("item_count", 5);
    rowVal.put("last_update_timestamp", Instant.now());
    data.add(rowVal);

    Rows rows = createRows(SHOPPING_CART.columns(), data);

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rows);

    Row one = resultSet.one();

    assertThat(one).isNotNull();
    assertRow(one, rows.rows.get(0));
  }

  @Test
  public void rows() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void rowsWithPaging() throws ExecutionException, InterruptedException {
    rowsSameUser.resultMetadata.pagingState = ByteBuffer.allocate(1);

    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    CompletableFuture<Result> future = mock(CompletableFuture.class);

    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> rowVal = new HashMap<>();
    rowVal.put("userid", "123");
    rowVal.put("item_count", 9);
    rowVal.put("last_update_timestamp", Instant.now());
    data.add(rowVal);
    Rows secondPage = createRows(SHOPPING_CART.columns(), data);
    when(future.get()).thenReturn(secondPage);

    when(connection.execute(any(), any(), anyLong())).thenReturn(future);

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            new SimpleStatement("select * from system.local;"),
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    rowsSameUser.rows.addAll(secondPage.rows);
    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void rowsWithPagingNoFilter() throws ExecutionException, InterruptedException {
    rowsSameUser.resultMetadata.pagingState = ByteBuffer.allocate(1);

    CompletableFuture<Result> future = mock(CompletableFuture.class);
    when(future.get()).thenReturn(rowsDifferentUser);

    when(connection.execute(any(), any(), anyLong())).thenReturn(future);

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            new SimpleStatement("select * from system.local;"),
            rowsSameUser);

    List<Row> rowsResult = resultSet.rows();

    rowsSameUser.rows.addAll(rowsDifferentUser.rows);
    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void rowsWithPagingNoClaim() throws ExecutionException, InterruptedException {
    rowsSameUser.resultMetadata.pagingState = ByteBuffer.allocate(1);

    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");

    CompletableFuture<Result> future = mock(CompletableFuture.class);
    when(future.get()).thenReturn(rowsDifferentUser);

    when(connection.execute(any(), any(), anyLong())).thenReturn(future);

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            new SimpleStatement("select * from system.local;"),
            rowsSameUser);
    resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    rowsSameUser.rows.addAll(rowsDifferentUser.rows);
    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void rowsWithPagingFiltered() throws ExecutionException, InterruptedException {
    rowsDifferentUser.resultMetadata.pagingState = ByteBuffer.allocate(1);

    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    @SuppressWarnings("unchecked")
    CompletableFuture<Result> future = mock(CompletableFuture.class);

    List<Map<String, Object>> data = new ArrayList<>();
    Map<String, Object> rowVal = new HashMap<>();
    rowVal.put("userid", "123");
    rowVal.put("item_count", 9);
    rowVal.put("last_update_timestamp", Instant.now());
    data.add(rowVal);
    Rows secondPage = createRows(SHOPPING_CART.columns(), data);
    when(future.get()).thenReturn(secondPage);

    when(connection.execute(any(), any(), anyLong())).thenReturn(future);

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            new SimpleStatement("select * from system.local;"),
            rowsDifferentUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    rowsDifferentUser.rows.remove(1);
    rowsDifferentUser.rows.add(secondPage.rows.get(0));
    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(2);
    assertRows(rowsResult, rowsDifferentUser.rows);
  }

  @Test
  public void rowsNotAuthorized() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "456");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(0);
  }

  @Test
  public void rowsNoClaims() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void rowsDifferentUserId() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsDifferentUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.rows();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(1);
    assertRow(rowsResult.get(0), rowsDifferentUser.rows.get(0));
  }

  @Test
  public void rowsNoFilter() {
    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsDifferentUser);

    List<Row> rowsResult = resultSet.rows();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(2);
    assertRows(rowsResult, rowsDifferentUser.rows);
  }

  @Test
  public void currentPageRows() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.currentPageRows();

    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void currentPageRowsNotAuthorized() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "456");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.currentPageRows();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(0);
  }

  @Test
  public void currentPageRowsNoClaims() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.currentPageRows();

    assertThat(rowsResult).isNotNull();
    assertRows(rowsResult, rowsSameUser.rows);
  }

  @Test
  public void currentPageRowsDifferentUserId() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsDifferentUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    List<Row> rowsResult = resultSet.currentPageRows();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(1);
    assertRow(rowsResult.get(0), rowsDifferentUser.rows.get(0));
  }

  @Test
  public void currentPageRowsNoFilter() {
    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsDifferentUser);

    List<Row> rowsResult = resultSet.currentPageRows();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.size()).isEqualTo(2);
    assertRows(rowsResult, rowsDifferentUser.rows);
  }

  @Test
  public void iterator() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Iterator<Row> rowsResult = resultSet.iterator();

    assertThat(rowsResult).isNotNull();
    int i = 0;
    while (rowsResult.hasNext()) {
      Row next = rowsResult.next();
      List<ByteBuffer> actualRow = rowsSameUser.rows.get(i);
      assertRow(next, actualRow);

      i++;
    }
  }

  @Test
  public void iteratorNotAuthorized() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "456");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Iterator<Row> rowsResult = resultSet.iterator();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.hasNext()).isFalse();
  }

  @Test
  public void iteratorNoClaims() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsSameUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Iterator<Row> rowsResult = resultSet.iterator();

    assertThat(rowsResult).isNotNull();
    int i = 0;
    while (rowsResult.hasNext()) {
      Row next = rowsResult.next();
      List<ByteBuffer> actualRow = rowsSameUser.rows.get(i);
      assertRow(next, actualRow);

      i++;
    }
  }

  @Test
  public void iteratorDifferentUserId() {
    Map<String, String> claims = new HashMap<>();
    claims.put("x-stargate-role", "web-user");
    claims.put("x-stargate-userid", "123");

    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsDifferentUser);
    resultSet = resultSet.withRowInspector(createAuthFilter(claims));

    Iterator<Row> rowsResult = resultSet.iterator();

    assertThat(rowsResult).isNotNull();
    assertThat(rowsResult.hasNext()).isTrue();
    assertRow(rowsResult.next(), rowsDifferentUser.rows.get(0));
    assertThat(rowsResult.hasNext()).isFalse();
  }

  @Test
  public void iteratorNoFilter() {
    ResultSet resultSet =
        new PersistenceBackedResultSet(
            connection,
            ImmutableParameters.builder()
                .protocolVersion(org.apache.cassandra.stargate.transport.ProtocolVersion.CURRENT)
                .build(),
            null,
            rowsDifferentUser);

    Iterator<Row> rowsResult = resultSet.iterator();

    assertThat(rowsResult).isNotNull();
    int i = 0;
    while (rowsResult.hasNext()) {
      Row next = rowsResult.next();
      List<ByteBuffer> actualRow = rowsDifferentUser.rows.get(i);
      assertRow(next, actualRow);

      i++;
    }
  }

  private void assertRows(List<Row> rowsResult, List<List<ByteBuffer>> rows) {
    for (int i = 0; i < rowsResult.size(); i++) {
      assertRow(rowsResult.get(i), rows.get(i));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void assertRow(Row expectedRow, List<ByteBuffer> actualRow) {
    for (int i = 0; i < expectedRow.columns().size(); i++) {
      TypeCodec codec = Objects.requireNonNull(expectedRow.columns().get(i).type()).codec();
      Object expected = expectedRow.get(i, codec);
      Object actual = codec.decode(actualRow.get(i), ProtocolVersion.DEFAULT);
      assertThat(expected).isEqualTo(actual);
    }
  }

  @SuppressWarnings({"unchecked"})
  private Rows createRows(List<Column> columns, List<Map<String, Object>> data) {
    List<List<ByteBuffer>> rows = new ArrayList<>();
    for (Map<String, Object> datum : data) {
      List<ByteBuffer> values = new ArrayList<>(columns.size());
      for (Column column : columns) {
        Object v = datum.get(column.name());
        values.add(
            v == null
                ? null
                : Objects.requireNonNull(column.type()).codec().encode(v, ProtocolVersion.DEFAULT));
      }
      rows.add(values);
    }

    return new Rows(rows, new ResultMetadata(null, columns, null, null));
  }

  private Predicate<Row> createAuthFilter(Map<String, String> claims) {
    String STARGATE_PREFIX = "x-stargate-";

    return row -> {
      if (row == null) {
        return true;
      }

      for (Column col : row.columns()) {
        if (claims.containsKey(STARGATE_PREFIX + col.name())) {
          String stargateClaimValue = claims.get(STARGATE_PREFIX + col.name());
          String columnValue = row.getString(col.name());
          if (!stargateClaimValue.equals(columnValue)) {
            return false;
          }
        }
      }
      return true;
    };
  }

  private static final Table SHOPPING_CART =
      ImmutableTable.builder()
          .keyspace("store")
          .name("shopping_cart")
          .addColumns(
              ImmutableColumn.builder()
                  .keyspace("store")
                  .table("shopping_cart")
                  .name("userid")
                  .type(Type.Text)
                  .kind(Kind.PartitionKey)
                  .build(),
              ImmutableColumn.builder()
                  .keyspace("store")
                  .table("shopping_cart")
                  .name("item_count")
                  .type(Type.Int)
                  .kind(Kind.Regular)
                  .build(),
              ImmutableColumn.builder()
                  .keyspace("store")
                  .table("shopping_cart")
                  .name("last_update_timestamp")
                  .type(Type.Timestamp)
                  .kind(Kind.Regular)
                  .build())
          .build();
}
