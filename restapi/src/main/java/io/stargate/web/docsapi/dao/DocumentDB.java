package io.stargate.web.docsapi.dao;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.Query;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.json.DeadLeaf;
import io.stargate.web.docsapi.service.query.DocsApiConstants;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentDB {
  private static final Logger logger = LoggerFactory.getLogger(DocumentDB.class);

  private static final Splitter PATH_SPLITTER = Splitter.on(".");

  private final Boolean useLoggedBatches;

  private final DataStore dataStore;
  private final AuthorizationService authorizationService;
  private final AuthenticationSubject authenticationSubject;
  private final QueryExecutor executor;
  private final DocsApiConfiguration config;
  private final List<String> allColumnNames;

  public DocumentDB(
      DataStore dataStore,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorizationService,
      DocsApiConfiguration config) {
    this.dataStore = dataStore;
    this.authenticationSubject = authenticationSubject;
    this.authorizationService = authorizationService;
    this.config = config;
    this.allColumnNames =
        Arrays.asList(DocsApiConstants.ALL_COLUMNS_NAMES.apply(config.getMaxDepth()));
    useLoggedBatches =
        Boolean.parseBoolean(
            System.getProperty(
                "stargate.document_use_logged_batches",
                Boolean.toString(dataStore.supportsLoggedBatches())));
    if (!dataStore.supportsSAI() && !dataStore.supportsSecondaryIndex()) {
      throw new IllegalStateException("Backend does not support any known index types.");
    }

    executor = new QueryExecutor(dataStore, config);
  }

  public QueryExecutor getQueryExecutor() {
    return executor;
  }

  public AuthorizationService getAuthorizationService() {
    return authorizationService;
  }

  public AuthenticationSubject getAuthenticationSubject() {
    return authenticationSubject;
  }

  public boolean supportsSAI() {
    return dataStore.supportsSAI();
  }

  public boolean treatBooleansAsNumeric() {
    return !dataStore.supportsSecondaryIndex();
  }

  public QueryBuilder queryBuilder() {
    return dataStore.queryBuilder();
  }

  public Schema schema() {
    return dataStore.schema();
  }

  public Table getTable(String keyspaceName, String table) {
    Keyspace keyspace = getKeyspace(keyspaceName);
    return (keyspace == null) ? null : keyspace.table(table);
  }

  public Keyspace getKeyspace(String keyspaceName) {
    return dataStore.schema().keyspace(keyspaceName);
  }

  public Set<Keyspace> getKeyspaces() {
    return dataStore.schema().keyspaces();
  }

  public void writeJsonSchemaToCollection(String namespace, String collection, String schemaData) {
    this.queryBuilder()
        .alter()
        .table(namespace, collection)
        .withComment(schemaData)
        .build()
        .execute()
        .join();
    this.dataStore.waitForSchemaAgreement();
  }

  /**
   * Creates the table described by @param tableName, in keyspace @keyspaceName, if it doesn't
   * already exist.
   *
   * @param keyspaceName
   * @param tableName
   * @return true if the table was created
   */
  public boolean maybeCreateTable(String keyspaceName, String tableName) {
    Keyspace ks = dataStore.schema().keyspace(keyspaceName);

    if (ks == null) {
      String message =
          String.format("Unknown namespace %s, you must create it first.", keyspaceName);
      throw new ErrorCodeRuntimeException(ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST, message);
    }

    // if table exists, do nothing
    Table table = ks.table(tableName);
    if (table != null) {
      return false;
    }

    // otherwise, check that the name is  valid
    if (!tableName.matches("^[a-zA-Z0-9_]+$")) {
      String message =
          String.format(
              "Could not create collection %s, it has invalid characters. Valid characters are alphanumeric and underscores.",
              tableName);
      throw new ErrorCodeRuntimeException(ErrorCode.DATASTORE_TABLE_NAME_INVALID, message);
    }

    // and create the table
    try {
      List<Column> columns = new ArrayList<>();
      columns.add(Column.create("key", Kind.PartitionKey, Type.Text));
      for (String columnName :
          DocsApiConstants.ALL_PATH_COLUMNS_NAMES.apply(config.getMaxDepth())) {
        columns.add(Column.create(columnName, Kind.Clustering, Type.Text));
      }
      columns.add(Column.create(DocsApiConstants.LEAF_COLUMN_NAME, Type.Text));
      columns.add(Column.create(DocsApiConstants.STRING_VALUE_COLUMN_NAME, Type.Text));
      columns.add(Column.create(DocsApiConstants.DOUBLE_VALUE_COLUMN_NAME, Type.Double));
      if (treatBooleansAsNumeric()) {
        columns.add(Column.create(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME, Type.Tinyint));
      } else {
        columns.add(Column.create(DocsApiConstants.BOOLEAN_VALUE_COLUMN_NAME, Type.Boolean));
      }
      dataStore
          .queryBuilder()
          .create()
          .table(keyspaceName, tableName)
          .column(columns)
          .build()
          .execute()
          .get();
      return true;
    } catch (AlreadyExistsException e) {
      logger.info("Table already exists, skipping creation", e);
      return false;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Unable to create schema for collection", e);
    }
  }

  public boolean maybeCreateTableIndexes(String keyspaceName, String tableName) {
    try {
      if (dataStore.supportsSAI()) {
        createSAIIndexes(keyspaceName, tableName);
      } else {
        createDefaultIndexes(keyspaceName, tableName);
      }
      return true;
    } catch (AlreadyExistsException e) {
      logger.info("Indexes already exist, skipping creation", e);
      return false;
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Unable to create indexes for collection " + tableName, e);
    }
  }

  /**
   * Drops indexes for `tableName` and adds SAI indexes in their place. Only works if `isDse` is
   * true.
   *
   * <p>This could cause performance degradation and/or disrupt in-flight requests, since indexes
   * are being dropped and re-created.
   */
  public boolean upgradeTableIndexes(String keyspaceName, String tableName) {
    if (!dataStore.supportsSAI()) {
      logger.info("Upgrade was attempted on a DataStore that does not support SAI.");
      return false;
    }

    dropTableIndexes(keyspaceName, tableName);
    return maybeCreateTableIndexes(keyspaceName, tableName);
  }

  /**
   * Drops indexes of `tableName` in preparation for replacing them with SAI. Note that the boolean
   * column index does not get altered, this is because SAI doesn't support booleans.
   *
   * @param keyspaceName The name of the keyspace containing the indexes that are being dropped.
   * @param tableName The name of the table used in the indexes that are being dropped.
   */
  public void dropTableIndexes(String keyspaceName, String tableName) {
    try {
      for (String name : DocsApiConstants.VALUE_COLUMN_NAMES) {
        dataStore
            .queryBuilder()
            .drop()
            .index(keyspaceName, tableName + "_" + name + "_idx")
            .ifExists()
            .build()
            .execute()
            .get();
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Unable to drop indexes in preparation for upgrade", e);
    }
  }

  /**
   * Checks that a table in a particular keyspace has the schema of a Documents collection. This is
   * done by checking that the table has all of the columns required.
   *
   * <p>Does not check that the table and keyspace exist.
   *
   * @param keyspaceName The name of the keyspace.
   * @param tableName The name of the table.
   */
  public boolean isDocumentsTable(String keyspaceName, String tableName) {
    List<String> columnNames =
        dataStore.schema().keyspace(keyspaceName).table(tableName).columns().stream()
            .map(Column::name)
            .collect(Collectors.toList());
    if (columnNames.size() != allColumnNames.size()) return false;
    columnNames.removeAll(allColumnNames);
    return columnNames.isEmpty();
  }

  public void tableExists(String keyspaceName, String tableName) {
    Keyspace keyspace = dataStore.schema().keyspace(keyspaceName);

    if (null == keyspace) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DATASTORE_KEYSPACE_DOES_NOT_EXIST,
          String.format("Namespace %s does not exist.", keyspaceName));
    }
    Table table = keyspace.table(tableName);
    if (null == table) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DATASTORE_TABLE_DOES_NOT_EXIST,
          String.format("Collection %s does not exist.", tableName));
    }
  }

  private void createDefaultIndexes(String keyspaceName, String tableName)
      throws InterruptedException, ExecutionException {
    for (String name : DocsApiConstants.VALUE_COLUMN_NAMES) {
      createDefaultIndex(keyspaceName, tableName, name);
    }
  }

  private void createDefaultIndex(String keyspaceName, String tableName, String columnName)
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

  private void createSAIIndexes(String keyspaceName, String tableName)
      throws InterruptedException, ExecutionException {
    for (String name : DocsApiConstants.VALUE_COLUMN_NAMES) {
      if (name.equals("bool_value") && dataStore.supportsSecondaryIndex()) {
        // SAI doesn't support booleans, so add a non-SAI index here.
        createDefaultIndex(keyspaceName, tableName, name);
      } else {
        // If the data store explicitly does not support secondary indexes,
        // it will use a tinyint to represent booleans and use SAI.
        dataStore
            .queryBuilder()
            .create()
            .index()
            .ifNotExists()
            .on(keyspaceName, tableName)
            .column(name)
            .custom("StorageAttachedIndex")
            .build()
            .execute()
            .get();
      }
    }
  }

  public void deleteTable(String keyspaceName, String tableName)
      throws InterruptedException, ExecutionException {
    dataStore.queryBuilder().drop().table(keyspaceName, tableName).build().execute().get();
  }

  public CompletableFuture<ResultSet> executeBatchAsync(
      Collection<BoundQuery> queries, ExecutionContext context) {
    queries.forEach(context::traceDeferredDml);

    if (useLoggedBatches) {
      return dataStore.batch(queries, ConsistencyLevel.LOCAL_QUORUM);
    } else {
      return dataStore.unloggedBatch(queries, ConsistencyLevel.LOCAL_QUORUM);
    }
  }

  /** Deletes from @param tableName all rows that are prefixed by @param pathPrefixToDelete */
  public BoundQuery getPrefixDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathPrefixToDelete) {

    List<BuiltCondition> where = new ArrayList<>(1 + pathPrefixToDelete.size());
    where.add(BuiltCondition.of("key", Predicate.EQ, key));
    for (int i = 0; i < pathPrefixToDelete.size(); i++) {
      where.add(BuiltCondition.of("p" + i, Predicate.EQ, pathPrefixToDelete.get(i)));
    }
    Query prepared =
        dataStore
            .prepare(
                dataStore
                    .queryBuilder()
                    .delete()
                    .from(keyspaceName, tableName)
                    .timestamp()
                    .where(where)
                    .build())
            .join();

    return prepared.bind(microsTimestamp);
  }

  /**
   * Prepares a delete from @param tableName with all rows that represent array elements at @param
   * pathToDelete, and also deletes all rows that match the @param patchedKeys at path @param
   * pathToDelete.
   */
  public BoundQuery getSubpathArrayDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathToDelete) {

    int pathSize = pathToDelete.size();
    List<BuiltCondition> where = new ArrayList<>(3 + pathSize);
    where.add(BuiltCondition.of("key", Predicate.EQ, key));
    for (int i = 0; i < pathSize; i++) {
      where.add(BuiltCondition.of("p" + i, Predicate.EQ, pathToDelete.get(i)));
    }
    // Delete array paths with a range tombstone
    where.add(BuiltCondition.of("p" + pathSize, Predicate.GTE, "[000000]"));
    where.add(BuiltCondition.of("p" + pathSize, Predicate.LTE, "[999999]"));
    Query prepared =
        dataStore
            .prepare(
                dataStore
                    .queryBuilder()
                    .delete()
                    .from(keyspaceName, tableName)
                    .timestamp()
                    .where(where)
                    .build())
            .join();
    return prepared.bind(microsTimestamp);
  }

  /**
   * Prepares a delete from @param tableName with all rows that match the @param keysToDelete at
   * path @param pathToDelete.
   */
  public BoundQuery getPathKeysDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathToDelete,
      List<String> keysToDelete) {

    int pathSize = pathToDelete.size();
    List<BuiltCondition> where = new ArrayList<>(3 + pathSize);
    where.add(BuiltCondition.of("key", Predicate.EQ, key));
    for (int i = 0; i < pathSize; i++) {
      where.add(BuiltCondition.of("p" + i, Predicate.EQ, pathToDelete.get(i)));
    }
    if (pathSize < config.getMaxDepth() && !keysToDelete.isEmpty()) {
      where.add(BuiltCondition.of("p" + pathSize, Predicate.IN, keysToDelete));
    }
    Query prepared =
        dataStore
            .prepare(
                dataStore
                    .queryBuilder()
                    .delete()
                    .from(keyspaceName, tableName)
                    .timestamp()
                    .where(where)
                    .build())
            .join();
    return prepared.bind(microsTimestamp);
  }

  public boolean authorizeDeleteDeadLeaves(String keyspaceName, String tableName) {
    try {
      getAuthorizationService()
          .authorizeDataWrite(
              getAuthenticationSubject(), keyspaceName, tableName, Scope.DELETE, SourceAPI.REST);
      return true;
    } catch (UnauthorizedException e) {
      logger.debug("Not authorized to delete dead leaves.", e);
      return false;
    }
  }

  public CompletableFuture<ResultSet> deleteDeadLeaves(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      Map<String, Set<DeadLeaf>> deadLeaves,
      ExecutionContext context) {

    List<BoundQuery> queries = new ArrayList<>();
    for (Map.Entry<String, Set<DeadLeaf>> entry : deadLeaves.entrySet()) {
      String path = entry.getKey();
      Set<DeadLeaf> leaves = entry.getValue();
      List<String> pathParts = PATH_SPLITTER.splitToList(path);
      List<String> pathToDelete = pathParts.subList(1, pathParts.size());

      boolean deleteArray = false;
      boolean deleteAll = false;
      List<String> keysToDelete = new ArrayList<>();
      for (DeadLeaf deadLeaf : leaves) {
        if (deadLeaf.getName().equals(DeadLeaf.STAR)) {
          deleteAll = true;
        } else if (deadLeaf.getName().equals(DeadLeaf.ARRAY)) {
          deleteArray = true;
        } else {
          keysToDelete.add(deadLeaf.getName());
        }
      }

      if (!keysToDelete.isEmpty()) {
        queries.add(
            getPathKeysDeleteStatement(
                keyspaceName, tableName, key, microsTimestamp, pathToDelete, keysToDelete));
      }

      if (deleteAll) {
        queries.add(
            getPrefixDeleteStatement(keyspaceName, tableName, key, microsTimestamp, pathToDelete));
      } else if (deleteArray) {
        queries.add(
            getSubpathArrayDeleteStatement(
                keyspaceName, tableName, key, microsTimestamp, pathToDelete));
      }
    }

    // Fire this off in a future
    return executeBatchAsync(queries, context.nested("ASYNC DOCUMENT CORRECTION"));
  }
}
