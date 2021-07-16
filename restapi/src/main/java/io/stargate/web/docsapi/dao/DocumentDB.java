package io.stargate.web.docsapi.dao;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.Scope;
import io.stargate.auth.SourceAPI;
import io.stargate.auth.UnauthorizedException;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.AbstractBound;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.query.builder.ValueModifier;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Keyspace;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.ExecutionContext;
import io.stargate.web.docsapi.service.QueryExecutor;
import io.stargate.web.docsapi.service.RawDocument;
import io.stargate.web.docsapi.service.json.DeadLeaf;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentDB {
  private static final Logger logger = LoggerFactory.getLogger(DocumentDB.class);
  private static final List<Column> allColumns;
  private static final List<String> allColumnNames;
  private static final List<Column.ColumnType> allColumnTypes;
  private static final List<String> allPathColumnNames;
  private static final List<Column.ColumnType> allPathColumnTypes;
  public static final int MAX_PAGE_SIZE = 20;
  public static final Integer MAX_DEPTH = Integer.getInteger("stargate.document_max_depth", 64);
  private Boolean useLoggedBatches;
  public static final Integer SEARCH_PAGE_SIZE =
      Integer.getInteger("stargate.document_search_page_size", 1000);

  // All array elements will be represented as 6 digits, so they get left-padded, such as [000010]
  // instead of [10]
  public static final Integer MAX_ARRAY_LENGTH =
      Integer.getInteger("stargate.document_max_array_len", 1000000);
  public static final String GLOB_VALUE = "*";
  public static final String GLOB_ARRAY_VALUE = "[*]";

  public static final String ROOT_DOC_MARKER = "DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900";
  public static final String EMPTY_OBJECT_MARKER = "EMPTYOBJ-bccbeee1-6173-4120-8492-7d7bafaefb1f";
  public static final String EMPTY_ARRAY_MARKER = "EMPTYARRAY-9df4802a-c135-42d6-8be3-d23d9520a4e7";

  private static final String[] VALUE_COLUMN_NAMES =
      new String[] {"leaf", "text_value", "dbl_value", "bool_value"};
  private static final Splitter PATH_SPLITTER = Splitter.on(".");

  final DataStore dataStore;
  private final AuthorizationService authorizationService;
  private final AuthenticationSubject authenticationSubject;
  private final QueryExecutor executor;

  static {
    allColumns = new ArrayList<>();
    allColumnNames = new ArrayList<>();
    allColumnTypes = new ArrayList<>();
    allPathColumnNames = new ArrayList<>();
    allPathColumnTypes = new ArrayList<>();
    allColumnNames.add("key");
    allColumnTypes.add(Type.Text);
    allColumns.add(Column.create("key", Type.Text));
    for (int i = 0; i < MAX_DEPTH; i++) {
      allPathColumnNames.add("p" + i);
      allPathColumnTypes.add(Type.Text);
      allColumns.add(Column.create("p" + i, Type.Text));
    }
    allColumnNames.addAll(allPathColumnNames);
    allColumnTypes.addAll(allPathColumnTypes);
    allColumnNames.add("leaf");
    allColumnTypes.add(Type.Text);
    allColumns.add(Column.create("leaf", Type.Text));
    allColumnNames.add("text_value");
    allColumnTypes.add(Type.Text);
    allColumns.add(Column.create("text_value", Type.Text));
    allColumnNames.add("dbl_value");
    allColumnTypes.add(Type.Double);
    allColumns.add(Column.create("dbl_value", Type.Double));
    allColumnNames.add("bool_value");
    allColumnTypes.add(Type.Boolean);
    allColumns.add(Column.create("bool_value", Type.Boolean));

    if (MAX_ARRAY_LENGTH > 1000000) {
      throw new IllegalStateException(
          "stargate.document_max_array_len cannot be greater than 1000000.");
    }
  }

  public DocumentDB(
      DataStore dataStore,
      AuthenticationSubject authenticationSubject,
      AuthorizationService authorizationService) {
    this.dataStore = dataStore;
    this.authenticationSubject = authenticationSubject;
    this.authorizationService = authorizationService;
    useLoggedBatches =
        Boolean.parseBoolean(
            System.getProperty(
                "stargate.document_use_logged_batches",
                Boolean.toString(dataStore.supportsLoggedBatches())));
    if (!dataStore.supportsSAI() && !dataStore.supportsSecondaryIndex()) {
      throw new IllegalStateException("Backend does not support any known index types.");
    }

    executor = new QueryExecutor(dataStore);
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

  public boolean treatBooleansAsNumeric() {
    return !dataStore.supportsSecondaryIndex();
  }

  public static boolean containsIllegalSequences(String x) {
    return x.contains("[") || x.contains(".");
  }

  public static List<Column> allColumns() {
    return allColumns;
  }

  public QueryBuilder builder() {
    return dataStore.queryBuilder();
  }

  public Schema schema() {
    return dataStore.schema();
  }

  public void writeJsonSchemaToCollection(String namespace, String collection, String schemaData) {
    this.builder()
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

    if (!tableName.matches("^[a-zA-Z0-9_]+$")) {
      String message =
          String.format(
              "Could not create collection %s, it has invalid characters. Valid characters are alphanumeric and underscores.",
              tableName);
      throw new ErrorCodeRuntimeException(ErrorCode.DATASTORE_TABLE_NAME_INVALID, message);
    }

    if (ks.table(tableName) != null) return false;

    try {
      List<Column> columns = new ArrayList<>();
      columns.add(Column.create("key", Kind.PartitionKey, Type.Text));
      for (String columnName : allPathColumnNames) {
        columns.add(Column.create(columnName, Kind.Clustering, Type.Text));
      }
      columns.add(Column.create("leaf", Type.Text));
      columns.add(Column.create("text_value", Type.Text));
      columns.add(Column.create("dbl_value", Type.Double));
      if (treatBooleansAsNumeric()) {
        columns.add(Column.create("bool_value", Type.Tinyint));
      } else {
        columns.add(Column.create("bool_value", Type.Boolean));
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
      for (String name : VALUE_COLUMN_NAMES) {
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
    for (String name : VALUE_COLUMN_NAMES) {
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
    for (String name : VALUE_COLUMN_NAMES) {
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

  public void executeBatch(Collection<BoundQuery> queries, ExecutionContext context) {
    queries.forEach(context::traceDeferredDml);

    if (useLoggedBatches) {
      dataStore.batch(queries, ConsistencyLevel.LOCAL_QUORUM).join();
    } else {
      dataStore.unloggedBatch(queries, ConsistencyLevel.LOCAL_QUORUM).join();
    }
  }

  public void authorizeSelect(String keyspace, String collection) throws UnauthorizedException {
    // Run generic authorizeDataRead for now
    getAuthorizationService()
        .authorizeDataRead(getAuthenticationSubject(), keyspace, collection, SourceAPI.REST);
  }

  public Flowable<RawDocument> executeSelect(
      String keyspace,
      String collection,
      List<BuiltCondition> predicates,
      int pageSize,
      ByteBuffer pagingState,
      ExecutionContext context) {
    return Single.fromCallable(
            () -> {
              // Run generic authorizeDataRead for now
              getAuthorizationService()
                  .authorizeDataRead(
                      getAuthenticationSubject(), keyspace, collection, SourceAPI.REST);
              return this.builder()
                  .select()
                  .column(DocumentDB.allColumns())
                  .writeTimeColumn("leaf")
                  .from(keyspace, collection)
                  .where(predicates)
                  .build()
                  .bind();
            })
        .flatMapPublisher(q -> executor.queryDocs(q, pageSize, pagingState, context));
  }

  public Flowable<RawDocument> executeSelect(
      AbstractBound<?> query, int pageSize, ByteBuffer pagingState, ExecutionContext context) {
    return executor.queryDocs(query, pageSize, pagingState, context);
  }

  public Flowable<RawDocument> executeSelect(
      int keyDepth,
      AbstractBound<?> query,
      int pageSize,
      ByteBuffer pagingState,
      ExecutionContext context) {
    return executor.queryDocs(keyDepth, query, pageSize, pagingState, context);
  }

  public BoundQuery getInsertStatement(
      String keyspaceName, String tableName, long microsTimestamp, Object[] columnValues) {

    List<ValueModifier> modifiers = new ArrayList<>(columnValues.length);
    for (int i = 0; i < columnValues.length; i++) {
      modifiers.add(ValueModifier.set(allColumnNames.get(i), columnValues[i]));
    }
    BoundQuery query =
        dataStore
            .queryBuilder()
            .insertInto(keyspaceName, tableName)
            .value(modifiers)
            .timestamp(microsTimestamp)
            .build()
            .bind();
    logger.debug(query.toString());
    return query;
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
    BoundQuery query =
        dataStore
            .queryBuilder()
            .delete()
            .from(keyspaceName, tableName)
            .timestamp(microsTimestamp)
            .where(where)
            .build()
            .bind();
    logger.debug(query.toString());
    return query;
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
    BoundQuery query =
        dataStore
            .queryBuilder()
            .delete()
            .from(keyspaceName, tableName)
            .timestamp(microsTimestamp)
            .where(where)
            .build()
            .bind();
    logger.debug(query.toString());
    return query;
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
    if (pathSize < MAX_DEPTH && !keysToDelete.isEmpty()) {
      where.add(BuiltCondition.of("p" + pathSize, Predicate.IN, keysToDelete));
    }
    BoundQuery query =
        dataStore
            .queryBuilder()
            .delete()
            .from(keyspaceName, tableName)
            .timestamp(microsTimestamp)
            .where(where)
            .build()
            .bind();
    logger.debug(query.toString());
    return query;
  }

  /** Deletes from @param tableName all rows that match @param pathToDelete exactly. */
  public BoundQuery getExactPathDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathToDelete) {

    int pathSize = pathToDelete.size();
    List<BuiltCondition> where = new ArrayList<>(1 + MAX_DEPTH);
    where.add(BuiltCondition.of("key", Predicate.EQ, key));
    for (int i = 0; i < pathSize; i++) {
      where.add(BuiltCondition.of("p" + i, Predicate.EQ, pathToDelete.get(i)));
    }
    for (int i = pathSize; i < MAX_DEPTH; i++) {
      where.add(BuiltCondition.of("p" + i, Predicate.EQ, ""));
    }
    BoundQuery query =
        dataStore
            .queryBuilder()
            .delete()
            .from(keyspaceName, tableName)
            .timestamp(microsTimestamp)
            .where(where)
            .build()
            .bind();
    logger.debug(query.toString());
    return query;
  }

  /**
   * Performs a delete of all the rows that are prefixed by the @param path, and then does an insert
   * using the @param vars provided, all in one batch.
   */
  public void deleteThenInsertBatch(
      String keyspace,
      String table,
      String key,
      List<Object[]> vars,
      List<String> pathToDelete,
      long microsSinceEpoch,
      ExecutionContext context)
      throws UnauthorizedException {

    List<BoundQuery> queries = new ArrayList<>(1 + vars.size());
    queries.add(getPrefixDeleteStatement(keyspace, table, key, microsSinceEpoch - 1, pathToDelete));

    for (Object[] values : vars) {
      queries.add(getInsertStatement(keyspace, table, microsSinceEpoch, values));
    }

    getAuthorizationService()
        .authorizeDataWrite(authenticationSubject, keyspace, table, Scope.DELETE, SourceAPI.REST);

    getAuthorizationService()
        .authorizeDataWrite(authenticationSubject, keyspace, table, Scope.MODIFY, SourceAPI.REST);

    executeBatch(queries, context);
  }

  /**
   * Performs a delete of all the rows that are prefixed by the @param path, and then does an insert
   * using the @param vars provided, all in one batch.
   */
  public void deleteManyThenInsertBatch(
      String keyspace,
      String table,
      List<String> keys,
      List<Object[]> vars,
      List<String> pathToDelete,
      long microsSinceEpoch,
      ExecutionContext context)
      throws UnauthorizedException {

    List<BoundQuery> queries = new ArrayList<>(keys.size() + vars.size());
    keys.forEach(
        key ->
            queries.add(
                getPrefixDeleteStatement(
                    keyspace, table, key, microsSinceEpoch - 1, pathToDelete)));

    for (Object[] values : vars) {
      queries.add(getInsertStatement(keyspace, table, microsSinceEpoch, values));
    }

    getAuthorizationService()
        .authorizeDataWrite(authenticationSubject, keyspace, table, Scope.DELETE, SourceAPI.REST);

    getAuthorizationService()
        .authorizeDataWrite(authenticationSubject, keyspace, table, Scope.MODIFY, SourceAPI.REST);

    executeBatch(queries, context);
  }

  /**
   * Performs a delete of all the rows that match exactly the @param path, deletes all array paths,
   * and then does an insert using the @param vars provided, all in one batch.
   */
  public void deletePatchedPathsThenInsertBatch(
      String keyspace,
      String table,
      String key,
      List<Object[]> vars,
      List<String> pathToDelete,
      List<String> patchedKeys,
      long microsSinceEpoch,
      ExecutionContext context)
      throws UnauthorizedException {
    boolean hasPath = !pathToDelete.isEmpty();

    long insertTs = microsSinceEpoch;
    long deleteTs = microsSinceEpoch - 1;

    List<BoundQuery> queries = new ArrayList<>(vars.size() + 3);
    for (Object[] values : vars) {
      queries.add(getInsertStatement(keyspace, table, insertTs, values));
    }

    if (hasPath) {
      // Only deleting the root path when there is a defined `pathToDelete` ensures that the DOCROOT
      // entry is never deleted out.
      queries.add(getExactPathDeleteStatement(keyspace, table, key, deleteTs, pathToDelete));
    }

    queries.add(getSubpathArrayDeleteStatement(keyspace, table, key, deleteTs, pathToDelete));
    queries.add(
        getPathKeysDeleteStatement(keyspace, table, key, deleteTs, pathToDelete, patchedKeys));

    Object[] deleteVarsWithPathKeys = new Object[pathToDelete.size() + patchedKeys.size() + 2];
    deleteVarsWithPathKeys[0] = microsSinceEpoch - 1;
    deleteVarsWithPathKeys[1] = key;
    for (int i = 0; i < pathToDelete.size(); i++) {
      deleteVarsWithPathKeys[i + 2] = pathToDelete.get(i);
    }
    for (int i = 0; i < patchedKeys.size(); i++) {
      deleteVarsWithPathKeys[i + 2 + pathToDelete.size()] = patchedKeys.get(i);
    }

    getAuthorizationService()
        .authorizeDataWrite(authenticationSubject, keyspace, table, Scope.DELETE, SourceAPI.REST);

    getAuthorizationService()
        .authorizeDataWrite(authenticationSubject, keyspace, table, Scope.MODIFY, SourceAPI.REST);

    executeBatch(queries, context);
  }

  public void delete(
      String keyspace, String table, String key, List<String> pathToDelete, long microsSinceEpoch)
      throws UnauthorizedException {

    getAuthorizationService()
        .authorizeDataWrite(
            getAuthenticationSubject(), keyspace, table, Scope.DELETE, SourceAPI.REST);
    dataStore
        .execute(
            getPrefixDeleteStatement(keyspace, table, key, microsSinceEpoch, pathToDelete),
            ConsistencyLevel.LOCAL_QUORUM)
        .join();
  }

  public void deleteDeadLeaves(
      String keyspaceName,
      String tableName,
      String key,
      Map<String, Set<DeadLeaf>> deadLeaves,
      ExecutionContext context,
      long now)
      throws UnauthorizedException {
    deleteDeadLeaves(keyspaceName, tableName, key, now, deadLeaves, context);
  }

  @VisibleForTesting
  void deleteDeadLeaves(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      Map<String, Set<DeadLeaf>> deadLeaves,
      ExecutionContext context)
      throws UnauthorizedException {

    getAuthorizationService()
        .authorizeDataWrite(
            getAuthenticationSubject(), keyspaceName, tableName, Scope.DELETE, SourceAPI.REST);

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
    executeBatch(queries, context.nested("ASYNC DOCUMENT CORRECTION"));
  }

  public Map<String, Object> newBindMap(List<String> path) {
    Map<String, Object> bindMap = new LinkedHashMap<>(MAX_DEPTH + 7);

    bindMap.put("key", TypedValue.UNSET);

    for (int i = 0; i < MAX_DEPTH; i++) {
      String value = "";
      if (i < path.size()) {
        value = path.get(i);
      }
      bindMap.put("p" + i, value);
    }

    bindMap.put("leaf", TypedValue.UNSET);
    bindMap.put("text_value", TypedValue.UNSET);
    bindMap.put("dbl_value", TypedValue.UNSET);
    bindMap.put("bool_value", TypedValue.UNSET);

    return bindMap;
  }
}
