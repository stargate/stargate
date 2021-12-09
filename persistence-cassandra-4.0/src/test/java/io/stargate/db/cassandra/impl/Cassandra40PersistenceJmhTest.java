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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.db.cassandra.impl;

import static io.stargate.db.cassandra.Cassandra40PersistenceActivator.makeConfig;

import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.TypedKeyValue;
import io.stargate.auth.UnauthorizedException;
import io.stargate.auth.entity.ResourceKind;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.SimpleStatement;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.DataStoreOptions;
import io.stargate.db.datastore.ImmutableDataStoreOptions;
import io.stargate.db.datastore.PersistenceBackedDataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.cassandra.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(
    value = 1,
    jvmArgs = {
      "-Dstargate.persistence_id=CassandraPersistence",
      "-Dstargate.cluster_name=stargate-v4",
      "-Dstargate.seed_list=127.0.0.1",
      "-Xms2g",
      "-Xmx2g"
    })
@State(Scope.Benchmark)
@Warmup(iterations = 3)
public class Cassandra40PersistenceJmhTest {

  public static final String KEYSPACE = "jmh";
  public static final String TABLE = "documents";

  @Param({"10"})
  private int batchStatements;

  @Param({"45"})
  private int maxDepth;

  private Persistence.Connection connection;
  private DataStore dataStore;
  private List<ValueModifier> docValueModifiers;
  private Query<? extends BoundQuery> insertPrepared;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    String host = "127.0.0." + RandomUtils.nextInt(2, 256);
    System.setProperty("stargate.listen_address", host);

    File baseDir = Files.createTempDirectory("stargate-c4-persistence-jmh-test").toFile();
    Config config = makeConfig(baseDir);

    Cassandra40Persistence persistence = new Cassandra40Persistence();
    persistence.initialize(config);
    persistence.setAuthorizationService(new AtomicReference<>(new NoopAuthorizationService()));

    ClientInfo clientInfo = new ClientInfo(new InetSocketAddress("127.0.0.1", 0), null);
    AuthenticatedUser user =
        AuthenticatedUser.of("cassandra", "91fa3b16-7155-4c44-8764-7d6484665805");
    connection = persistence.newConnection(clientInfo);
    connection.login(user);

    ImmutableDataStoreOptions dataStoreOptions =
        DataStoreOptions.builder()
            .alwaysPrepareQueries(true)
            .customProperties(Collections.singletonMap("Host", host))
            .build();
    dataStore = new PersistenceBackedDataStore(connection, dataStoreOptions);

    connection
        .execute(
            new SimpleStatement(
                "CREATE KEYSPACE IF NOT EXISTS jmh WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"),
            Parameters.defaults(),
            System.nanoTime())
        .get();

    // document DB
    createDocumentTable(KEYSPACE, TABLE);
    createDocumentIndexes(KEYSPACE, TABLE);

    docValueModifiers =
        Arrays.stream(DocsApiConstants.ALL_COLUMNS_NAMES.apply(maxDepth))
            .map(ValueModifier::marker)
            .collect(Collectors.toList());

    insertPrepared = dataStore.prepare(getDocumentInsertQuery()).join();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    connection
        .execute(new SimpleStatement("DROP KEYSPACE jmh"), Parameters.defaults(), System.nanoTime())
        .get();
  }

  // @Benchmark
  public void bindInsert(Blackhole blackhole) {
    Object[] insertBindValues = getInsertBindValues(Long.MAX_VALUE, "document-id", 128);
    BoundQuery bind = insertPrepared.bind(insertBindValues);
    blackhole.consume(bind);
  }

  // @Benchmark
  @Threads(Threads.MAX)
  public void bindInsertThreadsMax(Blackhole blackhole) {
    Object[] insertBindValues = getInsertBindValues(Long.MAX_VALUE, "document-id", 128);
    BoundQuery bind = insertPrepared.bind(insertBindValues);
    blackhole.consume(bind);
  }

  // @Benchmark
  @Threads(120)
  public void prepareAndExecuteDocumentBatchLoggedSingleOpThreads120(Blackhole blackhole) {
    CompletableFuture<?> result =
        getDocumentBatchStatement().thenCompose(queries -> dataStore.batch(queries));
    blackhole.consume(result.join());
  }

  // @Benchmark
  @Threads(Threads.MAX)
  public void prepareAndExecuteDocumentBatchLoggedSingleOpThreadsMaxCore(Blackhole blackhole) {
    CompletableFuture<?> result =
        getDocumentBatchStatement().thenCompose(queries -> dataStore.batch(queries));
    blackhole.consume(result.join());
  }

  // @Benchmark
  public void prepareAndExecuteDocumentBatchLoggedSingleOp(Blackhole blackhole) {
    CompletableFuture<?> result =
        getDocumentBatchStatement().thenCompose(queries -> dataStore.batch(queries));
    blackhole.consume(result.join());
  }

  // @Benchmark
  @Threads(120)
  public void prepareDocumentBatchSingleOp(Blackhole blackhole) {
    Collection<BoundQuery> query = getDocumentBatchStatement().join();
    blackhole.consume(query);
  }

  @Benchmark
  @OperationsPerInvocation(1000)
  public void prepareAndExecuteDocumentBatchLogged(Blackhole blackhole) {
    CompletableFuture<?>[] all = new CompletableFuture<?>[1000];
    for (int i = 0; i < 1000; i++) {
      all[i] = getDocumentBatchStatement().thenCompose(queries -> dataStore.batch(queries));
    }
    CompletableFuture<Void> result = CompletableFuture.allOf(all);
    blackhole.consume(result.join());
  }

  // @Benchmark
  @OperationsPerInvocation(1000)
  public void prepareAndExecuteDocumentBatchUnlogged(Blackhole blackhole) {
    CompletableFuture<?>[] all = new CompletableFuture<?>[1000];
    for (int i = 0; i < 1000; i++) {
      all[i] = getDocumentBatchStatement().thenCompose(batch -> dataStore.unloggedBatch(batch));
    }
    CompletableFuture<Void> result = CompletableFuture.allOf(all);
    blackhole.consume(result.join());
  }

  // @Benchmark
  @OperationsPerInvocation(1000)
  public void prepareDocumentBatch(Blackhole blackhole) {
    CompletableFuture<?>[] all = new CompletableFuture<?>[1000];
    for (int i = 0; i < 1000; i++) {
      all[i] = getDocumentBatchStatement();
    }
    CompletableFuture<Void> result = CompletableFuture.allOf(all);
    blackhole.consume(result.join());
  }

  private CompletableFuture<Collection<BoundQuery>> getDocumentBatchStatement() {
    BuiltQuery<?> insertQuery = getDocumentInsertQuery();
    CompletableFuture<? extends Query<?>> insertPrepare = dataStore.prepare(insertQuery);

    BuiltQuery<? extends BoundQuery> deleteQuery = getDocumentDeleteQuery();
    CompletableFuture<? extends Query<? extends BoundQuery>> deletePrepare =
        dataStore.prepare(deleteQuery);

    return CompletableFuture.allOf(insertPrepare, deletePrepare)
        .thenApply(
            v -> {
              Collection<BoundQuery> queries = new ArrayList<>(batchStatements + 1);
              long timestamp = System.currentTimeMillis();
              String documentId = UUID.randomUUID().toString();

              // first delete
              BoundQuery deleteBound = deletePrepare.join().bind(timestamp - 1, documentId);
              queries.add(deleteBound);

              // then set of inserts
              for (int i = 0; i < batchStatements; i++) {
                Object[] values = getInsertBindValues(timestamp, documentId, i);

                BoundQuery bound = insertPrepare.join().bind(values);
                queries.add(bound);
              }
              return queries;
            });
  }

  private Object[] getInsertBindValues(long timestamp, String documentId, int i) {
    Object[] values = new Object[maxDepth + 6];
    // key at index 0
    values[0] = documentId;

    // then the path, based on the max depth
    for (int j = 0; j < maxDepth; j++) {
      if (j == 0) {
        values[j + 1] = String.valueOf(i);
      } else {
        values[j + 1] = "";
      }
    }

    // rest at the end
    values[maxDepth + 1] = String.valueOf(i);
    values[maxDepth + 2] = RandomStringUtils.randomAlphabetic(8);
    values[maxDepth + 3] = null;
    values[maxDepth + 4] = null;

    // respect the timestamp
    values[maxDepth + 5] = timestamp;
    return values;
  }

  private BuiltQuery<? extends BoundQuery> getDocumentDeleteQuery() {
    return dataStore
        .queryBuilder()
        .delete()
        .from(KEYSPACE, TABLE)
        .timestamp()
        .where(DocsApiConstants.KEY_COLUMN_NAME, Predicate.EQ)
        .build();
  }

  private BuiltQuery<? extends BoundQuery> getDocumentInsertQuery() {
    return dataStore
        .queryBuilder()
        .insertInto(KEYSPACE, TABLE)
        .value(docValueModifiers)
        .timestamp()
        .build();
  }

  private void createDocumentTable(String keyspaceName, String tableName)
      throws InterruptedException, ExecutionException {
    List<Column> columns = new ArrayList<>();
    columns.add(
        Column.create(
            DocsApiConstants.KEY_COLUMN_NAME, Column.Kind.PartitionKey, Column.Type.Text));
    for (String columnName : DocsApiConstants.ALL_PATH_COLUMNS_NAMES.apply(maxDepth)) {
      columns.add(Column.create(columnName, Column.Kind.Clustering, Column.Type.Text));
    }
    columns.add(Column.create(DocsApiConstants.LEAF_COLUMN_NAME, Column.Type.Text));
    columns.add(Column.create(DocsApiConstants.STRING_VALUE_COLUMN_NAME, Column.Type.Text));
    columns.add(Column.create(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME, Column.Type.Double));
    if (!dataStore.supportsSecondaryIndex()) {
      columns.add(Column.create(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME, Column.Type.Tinyint));
    } else {
      columns.add(Column.create(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME, Column.Type.Boolean));
    }
    dataStore
        .queryBuilder()
        .create()
        .table(keyspaceName, tableName)
        .ifNotExists(true)
        .column(columns)
        .build()
        .execute()
        .get();
  }

  private void createDocumentIndexes(String keyspaceName, String tableName)
      throws InterruptedException, ExecutionException {
    for (String name : DocsApiConstants.VALUE_COLUMN_NAMES) {
      createDocumentIndex(keyspaceName, tableName, name);
    }
  }

  private void createDocumentIndex(String keyspaceName, String tableName, String columnName)
      throws InterruptedException, ExecutionException {
    dataStore
        .queryBuilder()
        .create()
        .index()
        .ifNotExists()
        .on(keyspaceName, tableName)
        .column(columnName)
        .build()
        .execute()
        .get();
  }

  private static class NoopAuthorizationService implements AuthorizationService {

    @Override
    public ResultSet authorizedDataRead(
        Callable<ResultSet> action,
        AuthenticationSubject authenticationSubject,
        String keyspace,
        String table,
        List<TypedKeyValue> typedKeyValues,
        SourceAPI sourceAPI)
        throws Exception {
      return action.call();
    }

    @Override
    public void authorizeDataRead(
        AuthenticationSubject authenticationSubject,
        String keyspace,
        String table,
        SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizeDataWrite(
        AuthenticationSubject authenticationSubject,
        String keyspace,
        String table,
        io.stargate.auth.Scope scope,
        SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizeDataWrite(
        AuthenticationSubject authenticationSubject,
        String keyspace,
        String table,
        List<TypedKeyValue> typedKeyValues,
        io.stargate.auth.Scope scope,
        SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizeSchemaRead(
        AuthenticationSubject authenticationSubject,
        List<String> keyspaceNames,
        List<String> tableNames,
        SourceAPI sourceAPI,
        ResourceKind resource)
        throws UnauthorizedException {}

    @Override
    public void authorizeSchemaWrite(
        AuthenticationSubject authenticationSubject,
        String keyspace,
        String table,
        io.stargate.auth.Scope scope,
        SourceAPI sourceAPI,
        ResourceKind resource)
        throws UnauthorizedException {}

    @Override
    public void authorizeRoleManagement(
        AuthenticationSubject authenticationSubject,
        String role,
        io.stargate.auth.Scope scope,
        SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizeRoleManagement(
        AuthenticationSubject authenticationSubject,
        String role,
        String grantee,
        io.stargate.auth.Scope scope,
        SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizeRoleRead(
        AuthenticationSubject authenticationSubject, String role, SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizePermissionManagement(
        AuthenticationSubject authenticationSubject,
        String resource,
        String grantee,
        io.stargate.auth.Scope scope,
        SourceAPI sourceAPI)
        throws UnauthorizedException {}

    @Override
    public void authorizePermissionRead(
        AuthenticationSubject authenticationSubject, String role, SourceAPI sourceAPI)
        throws UnauthorizedException {}
  }
}
