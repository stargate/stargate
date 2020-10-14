package io.stargate.web.docsapi.dao;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Keyspace;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentDB {
  private static final Logger logger = LoggerFactory.getLogger(DocumentDB.class);
  private static final List<Character> forbiddenCharacters;
  private static final List<String> allColumnNames;
  private static final List<Column.ColumnType> allColumnTypes;
  private static final List<String> allPathColumnNames;
  private static final List<Column.ColumnType> allPathColumnTypes;
  public static final Integer MAX_DEPTH = Integer.getInteger("stargate.document_max_depth", 64);

  // All array elements will be represented as 6 digits, so they get left-padded, such as [000010]
  // instead of [10]
  public static final Integer MAX_ARRAY_LENGTH =
      Integer.getInteger("stargate.document_max_array_len", 1000000);
  public static final String GLOB_VALUE = "*";

  public static final String ROOT_DOC_MARKER = "DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900";
  public static final String EMPTY_OBJECT_MARKER = "EMPTYOBJ-bccbeee1-6173-4120-8492-7d7bafaefb1f";
  public static final String EMPTY_ARRAY_MARKER = "EMPTYARRAY-9df4802a-c135-42d6-8be3-d23d9520a4e7";

  final DataStore dataStore;

  static {
    allColumnNames = new ArrayList<>();
    allColumnTypes = new ArrayList<>();
    allPathColumnNames = new ArrayList<>();
    allPathColumnTypes = new ArrayList<>();
    allColumnNames.add("key");
    allColumnTypes.add(Type.Text);
    for (int i = 0; i < MAX_DEPTH; i++) {
      allPathColumnNames.add("p" + i);
      allPathColumnTypes.add(Type.Text);
    }
    allColumnNames.addAll(allPathColumnNames);
    allColumnTypes.addAll(allPathColumnTypes);
    allColumnNames.add("leaf");
    allColumnTypes.add(Type.Text);
    allColumnNames.add("text_value");
    allColumnTypes.add(Type.Text);
    allColumnNames.add("dbl_value");
    allColumnTypes.add(Type.Double);
    allColumnNames.add("bool_value");
    allColumnTypes.add(Type.Boolean);

    forbiddenCharacters = ImmutableList.of('[', ']', ',', '.', '\'', '*');

    if (MAX_ARRAY_LENGTH > 1000000) {
      throw new IllegalStateException(
          "stargate.document_max_array_len cannot be greater than 1000000.");
    }
  }

  public DocumentDB(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public static List<String> getForbiddenCharactersMessage() {
    return forbiddenCharacters.stream()
        .map(ch -> (new StringBuilder().append("`").append(ch).append("`").toString()))
        .collect(Collectors.toList());
  }

  public static boolean containsIllegalChars(String x) {
    return forbiddenCharacters.stream().anyMatch(ch -> x.indexOf(ch) >= 0);
  }

  public static List<Column> allColumns() {
    List<Column> allColumns = new ArrayList<>(allColumnNames.size());
    for (int i = 0; i < allColumnNames.size(); i++) {
      allColumns.add(Column.create(allColumnNames.get(i), allColumnTypes.get(i)));
    }
    return allColumns;
  }

  public QueryBuilder builder() {
    return dataStore.query();
  }

  public void maybeCreateTable(String keyspaceName, String tableName) {
    Keyspace ks = dataStore.schema().keyspace(keyspaceName);

    if (ks == null)
      throw new DocumentAPIRequestException(
          String.format("Unknown namespace %s, you must create it first.", keyspaceName));

    if (!tableName.matches("^[a-zA-Z0-9_]+$")) {
      throw new DocumentAPIRequestException(
          String.format(
              "Could not create collection %s, it has invalid characters. Valid characters are alphanumeric and underscores.",
              tableName));
    }

    if (ks.table(tableName) != null) return;

    try {
      dataStore
          .query(
              String.format(
                  "CREATE TABLE \"%s\".\"%s\" (key text, %s text, leaf text, text_value text, dbl_value double, bool_value boolean, PRIMARY KEY(key, %s))",
                  keyspaceName,
                  tableName,
                  String.join(" text, ", allPathColumnNames),
                  String.join(", ", allPathColumnNames)))
          .get();

      dataStore
          .query(String.format("CREATE INDEX ON \"%s\".\"%s\"(leaf)", keyspaceName, tableName))
          .get();

      dataStore
          .query(
              String.format("CREATE INDEX ON \"%s\".\"%s\"(text_value)", keyspaceName, tableName))
          .get();

      dataStore
          .query(String.format("CREATE INDEX ON \"%s\".\"%s\"(dbl_value)", keyspaceName, tableName))
          .get();

      dataStore
          .query(
              String.format("CREATE INDEX ON \"%s\".\"%s\"(bool_value)", keyspaceName, tableName))
          .get();
    } catch (AlreadyExistsException e) {
      // fine
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Unable to create schema for collection", e);
    }
  }

  public ResultSet executeSelect(String keyspace, String collection, List<Where<Object>> predicates)
      throws ExecutionException, InterruptedException {
    return this.builder()
        .select()
        .column(DocumentDB.allColumns())
        .from(keyspace, collection)
        .where(predicates)
        .withWriteTimeColumn("leaf")
        .execute();
  }

  public ResultSet executeSelect(
      String keyspace, String collection, List<Where<Object>> predicates, boolean allowFiltering)
      throws ExecutionException, InterruptedException {
    return this.builder()
        .select()
        .column(DocumentDB.allColumns())
        .from(keyspace, collection)
        .where(predicates)
        .allowFiltering(allowFiltering)
        .withWriteTimeColumn("leaf")
        .execute();
  }

  public ResultSet executeSelectAll(String keyspace, String collection)
      throws ExecutionException, InterruptedException {
    return this.builder()
        .select()
        .column(DocumentDB.allColumns())
        .from(keyspace, collection)
        .withWriteTimeColumn("leaf")
        .execute();
  }

  private PreparedStatement prepare(String statement) {
    return dataStore.prepare(statement).join();
  }

  public PreparedStatement.Bound getInsertStatement(
      String keyspaceName, String tableName, long microsTimestamp, Object[] columnValues) {
    String statement =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (:%s) USING TIMESTAMP ?",
            keyspaceName,
            tableName,
            String.join(", ", allColumnNames),
            String.join(", :", allColumnNames));

    logger.debug(statement);
    Object[] values = Arrays.copyOf(columnValues, columnValues.length + 1);
    values[values.length - 1] = microsTimestamp;
    return prepare(statement).bind(values);
  }

  /** Deletes from @param tableName all rows that are prefixed by @param pathPrefixToDelete */
  public PreparedStatement.Bound getPrefixDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathPrefixToDelete) {
    Object[] values = new Object[2 + pathPrefixToDelete.size()];
    values[0] = microsTimestamp;
    values[1] = key;
    StringBuilder pathClause = new StringBuilder();
    for (int i = 0; i < pathPrefixToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
      values[2 + i] = pathPrefixToDelete.get(i);
    }
    String statement =
        String.format(
            "DELETE FROM \"%s\".\"%s\" USING TIMESTAMP ? WHERE key = :key%s",
            keyspaceName, tableName, pathClause.toString());
    logger.debug(statement);
    return prepare(statement).bind(values);
  }

  /**
   * Prepares a delete from @param tableName with all rows that represent array elements at @param
   * pathToDelete, and also deletes all rows that match the @param patchedKeys at path @param
   * pathToDelete.
   */
  public PreparedStatement.Bound getSubpathArrayDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathToDelete) {
    Object[] values = new Object[2 + pathToDelete.size()];
    values[0] = microsTimestamp;
    values[1] = key;

    StringBuilder pathClause = new StringBuilder();
    for (int i = 0; i < pathToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
      values[2 + i] = pathToDelete.get(i);
    }

    // Delete array paths with a range tombstone
    pathClause
        .append(" AND p")
        .append(pathToDelete.size())
        .append(" >= '[000000]' AND p")
        .append(pathToDelete.size())
        .append(" <= '[999999]'");

    String statement =
        String.format(
            "DELETE FROM \"%s\".\"%s\" USING TIMESTAMP ? WHERE key = :key%s ",
            keyspaceName, tableName, pathClause.toString());

    logger.debug(statement);
    return prepare(statement).bind(values);
  }

  /**
   * Prepares a delete from @param tableName with all rows that match the @param keysToDelete at
   * path @param pathToDelete.
   */
  public PreparedStatement.Bound getPathKeysDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathToDelete,
      List<String> keysToDelete) {
    Object[] values = new Object[2 + pathToDelete.size() + keysToDelete.size()];
    int idx = 0;
    values[idx++] = microsTimestamp;
    values[idx++] = key;

    StringBuilder pathClause = new StringBuilder();
    for (int i = 0; i < pathToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
      values[idx++] = pathToDelete.get(i);
    }

    if (pathToDelete.size() < MAX_DEPTH && !keysToDelete.isEmpty()) {
      pathClause.append(" AND p").append(pathToDelete.size()).append(" IN (");
      for (int j = 0; j < keysToDelete.size(); j++) {
        pathClause.append(":p" + (pathToDelete.size() + j));
        values[idx++] = keysToDelete.get(j);
        if (j != keysToDelete.size() - 1) {
          pathClause.append(",");
        }
      }
    }

    pathClause.append(")");

    String statement =
        String.format(
            "DELETE FROM \"%s\".\"%s\" USING TIMESTAMP ? WHERE key = :key%s ",
            keyspaceName, tableName, pathClause.toString());

    // We might not have filled the whole values array pathToDelete >= MAX_DEPTH.
    if (idx < values.length) {
      values = Arrays.copyOf(values, idx);
    }
    logger.debug(statement);
    return prepare(statement).bind(values);
  }

  /** Deletes from @param tableName all rows that match @param pathToDelete exactly. */
  public PreparedStatement.Bound getExactPathDeleteStatement(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      List<String> pathToDelete) {
    Object[] values = new Object[2 + pathToDelete.size()];
    values[0] = microsTimestamp;
    values[1] = key;
    StringBuilder pathClause = new StringBuilder();
    int i = 0;
    for (; i < pathToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
      values[2 + i] = pathToDelete.get(i);
    }

    for (; i < MAX_DEPTH; i++) {
      pathClause.append(" AND p").append(i).append(" = ''");
    }

    String statement =
        String.format(
            "DELETE FROM \"%s\".\"%s\" USING TIMESTAMP ?  WHERE key = :key%s",
            keyspaceName, tableName, pathClause.toString());

    logger.debug(statement);
    return prepare(statement).bind(values);
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
      long microsSinceEpoch) {

    List<PreparedStatement.Bound> statements = new ArrayList<>(1 + vars.size());
    statements.add(
        getPrefixDeleteStatement(keyspace, table, key, microsSinceEpoch - 1, pathToDelete));

    for (Object[] values : vars) {
      statements.add(getInsertStatement(keyspace, table, microsSinceEpoch, values));
    }

    dataStore.batch(statements, ConsistencyLevel.LOCAL_QUORUM).join();
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
      long microsSinceEpoch) {
    boolean hasPath = !pathToDelete.isEmpty();

    long insertTs = microsSinceEpoch;
    long deleteTs = microsSinceEpoch - 1;

    List<PreparedStatement.Bound> statements = new ArrayList<>(vars.size() + 3);
    for (Object[] values : vars) {
      statements.add(getInsertStatement(keyspace, table, insertTs, values));
    }

    if (hasPath) {
      // Only deleting the root path when there is a defined `pathToDelete` ensures that the DOCROOT
      // entry is never deleted out.
      statements.add(getExactPathDeleteStatement(keyspace, table, key, deleteTs, pathToDelete));
    }

    statements.add(getSubpathArrayDeleteStatement(keyspace, table, key, deleteTs, pathToDelete));
    statements.add(
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

    dataStore.batch(statements, ConsistencyLevel.LOCAL_QUORUM).join();
  }

  public void delete(
      String keyspace, String table, String key, List<String> pathToDelete, long microsSinceEpoch) {

    getPrefixDeleteStatement(keyspace, table, key, microsSinceEpoch, pathToDelete).execute().join();
  }

  public void deleteDeadLeaves(
      String keyspaceName, String tableName, String key, Map<String, List<JsonNode>> deadLeaves) {
    long now = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    deleteDeadLeaves(keyspaceName, tableName, key, now, deadLeaves);
  }

  @VisibleForTesting
  void deleteDeadLeaves(
      String keyspaceName,
      String tableName,
      String key,
      long microsTimestamp,
      Map<String, List<JsonNode>> deadLeaves) {

    List<PreparedStatement.Bound> statements = new ArrayList<>();
    for (Map.Entry<String, List<JsonNode>> entry : deadLeaves.entrySet()) {
      String path = entry.getKey();
      List<JsonNode> deadNodes = entry.getValue();
      String[] pathParts = path.split("\\.");
      String[] pathToDelete = Arrays.copyOfRange(pathParts, 1, pathParts.length);

      boolean deleteArray = false;
      List<String> keysToDelete = new ArrayList<>();
      for (JsonNode deadNode : deadNodes) {
        if (deadNode.isArray()) {
          deleteArray = true;
        } else {
          Iterator<String> it = deadNode.fieldNames();
          while (it.hasNext()) {
            keysToDelete.add(it.next());
          }
        }
      }

      if (!keysToDelete.isEmpty()) {
        statements.add(
            getPathKeysDeleteStatement(
                keyspaceName,
                tableName,
                key,
                microsTimestamp,
                Arrays.asList(pathToDelete),
                keysToDelete));
      }

      if (deleteArray) {
        statements.add(
            getSubpathArrayDeleteStatement(
                keyspaceName, tableName, key, microsTimestamp, Arrays.asList(pathToDelete)));
      }
    }

    // Fire this off in a future
    dataStore.batch(statements, ConsistencyLevel.LOCAL_QUORUM);
  }

  public Map<String, Object> newBindMap(List<String> path) {
    Map<String, Object> bindMap = new LinkedHashMap<>(MAX_DEPTH + 7);

    bindMap.put("key", DataStore.UNSET);

    for (int i = 0; i < MAX_DEPTH; i++) {
      String value = "";
      if (i < path.size()) {
        value = path.get(i);
      }
      bindMap.put("p" + i, value);
    }

    bindMap.put("leaf", DataStore.UNSET);
    bindMap.put("text_value", DataStore.UNSET);
    bindMap.put("dbl_value", DataStore.UNSET);
    bindMap.put("bool_value", DataStore.UNSET);

    return bindMap;
  }
}
