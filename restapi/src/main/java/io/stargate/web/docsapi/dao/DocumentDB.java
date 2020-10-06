package io.stargate.web.docsapi.dao;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.PreparedStatement;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.query.Parameter;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.datastore.query.Where;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Keyspace;
import io.stargate.web.docsapi.exception.SchemalessRequestException;
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
  private static final List<String> allPathColumnNames;
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
    allPathColumnNames = new ArrayList<>();
    allColumnNames.add("key");
    for (int i = 0; i < MAX_DEPTH; i++) {
      allPathColumnNames.add("p" + i);
    }
    allColumnNames.addAll(allPathColumnNames);
    allColumnNames.add("leaf");
    allColumnNames.add("text_value");
    allColumnNames.add("dbl_value");
    allColumnNames.add("bool_value");

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
    return allColumnNames.stream().map(Column::reference).collect(Collectors.toList());
  }

  public QueryBuilder builder() {
    return dataStore.query();
  }

  public void maybeCreateTable(String keyspaceName, String tableName) {
    Keyspace ks = dataStore.schema().keyspace(keyspaceName);

    if (ks == null)
      throw new SchemalessRequestException(
          String.format(
              "Unknown namespace %s, you must create it first by creating a keyspace with the same name.",
              keyspaceName));

    if (!tableName.matches("^[a-zA-Z0-9_]+$")) {
      throw new SchemalessRequestException(
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

  public PreparedStatement getInsertStatement(String keyspaceName, String tableName) {
    String statement =
        String.format(
            "INSERT INTO \"%s\".\"%s\" (%s) VALUES (:%s) USING TIMESTAMP ?",
            keyspaceName,
            tableName,
            String.join(", ", allColumnNames),
            String.join(", :", allColumnNames));

    logger.debug(statement);
    return dataStore.prepare(statement);
  }

  /** Deletes from @param tableName all rows that are prefixed by @param pathPrefixToDelete */
  public PreparedStatement getPrefixDeleteStatement(
      String keyspaceName, String tableName, List<String> pathPrefixToDelete) {
    StringBuilder pathClause = new StringBuilder();
    for (int i = 0; i < pathPrefixToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
    }
    String statement =
        String.format(
            "DELETE FROM \"%s\".\"%s\" USING TIMESTAMP ? WHERE key = :key%s",
            keyspaceName, tableName, pathClause.toString());
    logger.debug(statement);
    return dataStore.prepare(statement);
  }

  /**
   * Prepares a delete from @param tableName with all rows that represent array elements at @param
   * pathToDelete, and also deletes all rows that match the @param patchedKeys at path @param
   * pathToDelete.
   */
  public PreparedStatement getSubpathArrayDeleteStatement(
      String keyspaceName, String tableName, List<String> pathToDelete) {
    StringBuilder pathClause = new StringBuilder();
    int i = 0;
    for (; i < pathToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
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
    return dataStore.prepare(statement);
  }

  /**
   * Prepares a delete from @param tableName with all rows that match the @param keysToDelete at
   * path @param pathToDelete.
   */
  public PreparedStatement getPathKeysDeleteStatement(
      String keyspaceName, String tableName, List<String> pathToDelete, List<String> keysToDelete) {
    StringBuilder pathClause = new StringBuilder();
    for (int i = 0; i < pathToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
    }

    if (pathToDelete.size() < MAX_DEPTH && !keysToDelete.isEmpty()) {
      pathClause.append(" AND p").append(pathToDelete.size()).append(" IN (");
      for (int j = 0; j < keysToDelete.size(); j++) {
        pathClause.append(":p" + pathToDelete.size());
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

    logger.debug(statement);
    return dataStore.prepare(statement);
  }

  /** Deletes from @param tableName all rows that match @param pathToDelete exactly. */
  public PreparedStatement getExactPathDeleteStatement(
      String keyspaceName, String tableName, List<String> pathToDelete) {
    StringBuilder pathClause = new StringBuilder();
    int i = 0;
    for (; i < pathToDelete.size(); i++) {
      pathClause.append(" AND p").append(i).append(" = :p").append(i);
    }

    for (; i < MAX_DEPTH; i++) {
      pathClause.append(" AND p").append(i).append(" = ''");
    }

    String statement =
        String.format(
            "DELETE FROM \"%s\".\"%s\" USING TIMESTAMP ?  WHERE key = :key%s",
            keyspaceName, tableName, pathClause.toString());

    logger.debug(statement);
    return dataStore.prepare(statement);
  }

  /**
   * Prepares an insert for the root-most part of a document. Adds the `IF NOT EXISTS` clause
   * if @param ifNotExists is true. If it is false, adds the `USING TIMESTAMP` clause. Inserts do
   * not support both `IF NOT EXISTS` and `USING TIMESTAMP`.
   */
  public PreparedStatement getRootDocInsertStatement(
      String keyspaceName, String tableName, boolean ifNotExists) {
    StringBuilder emptyStrings = new StringBuilder();

    for (int i = 0; i < MAX_DEPTH; i++) {
      emptyStrings.append("''");
      if (i < MAX_DEPTH - 1) {
        emptyStrings.append(", ");
      }
    }

    StringBuilder stmt = new StringBuilder();
    stmt.append("INSERT INTO \"%s\".\"%s\" (key, %s, leaf) VALUES (:key, %s, '%s')");
    if (ifNotExists) {
      stmt.append(" IF NOT EXISTS");
    } else {
      stmt.append(" USING TIMESTAMP ?");
    }

    String statement =
        String.format(
            stmt.toString(),
            keyspaceName,
            tableName,
            String.join(", ", allPathColumnNames),
            emptyStrings,
            ROOT_DOC_MARKER);

    logger.debug(statement);
    return dataStore.prepare(statement);
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
    PreparedStatement[] statements = new PreparedStatement[vars.size() + 2];
    statements[0] = getPrefixDeleteStatement(keyspace, table, pathToDelete);
    statements[1] = getRootDocInsertStatement(keyspace, table, false);
    Object[] deleteVars = new Object[pathToDelete.size() + 2];
    Object[] rootDocVars = new Object[2];
    deleteVars[0] = microsSinceEpoch - 1;
    deleteVars[1] = key;

    // Read the CQL in getRootDocInsertStatement to see why these are reversed
    rootDocVars[0] = key;
    rootDocVars[1] = microsSinceEpoch;

    for (int i = 2; i < deleteVars.length; i++) {
      deleteVars[i] = pathToDelete.get(i - 2);
    }

    vars.add(0, rootDocVars);
    vars.add(0, deleteVars);
    Arrays.fill(statements, 2, statements.length, getInsertStatement(keyspace, table));
    dataStore
        .processBatch(Arrays.asList(statements), vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM))
        .join();
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
    PreparedStatement[] statements = new PreparedStatement[vars.size() + (hasPath ? 4 : 3)];
    Object[] deleteVarsWithPathKeys = new Object[pathToDelete.size() + patchedKeys.size() + 2];
    Object[] deleteVarsExact = new Object[pathToDelete.size() + 2];
    Object[] rootDocInsertVars = new Object[2];
    deleteVarsWithPathKeys[0] = microsSinceEpoch - 1;
    deleteVarsWithPathKeys[1] = key;
    deleteVarsExact[0] = microsSinceEpoch - 1;
    deleteVarsExact[1] = key;

    // Read the CQL in getRootDocInsertStatement to see why these are reversed
    rootDocInsertVars[0] = key;
    rootDocInsertVars[1] = microsSinceEpoch;

    for (int i = 0; i < pathToDelete.size(); i++) {
      deleteVarsWithPathKeys[i + 2] = pathToDelete.get(i);
      deleteVarsExact[i + 2] = pathToDelete.get(i);
    }

    for (int i = 0; i < patchedKeys.size(); i++) {
      deleteVarsWithPathKeys[i + 2 + pathToDelete.size()] = patchedKeys.get(i);
    }

    Arrays.fill(
        statements, 0, statements.length - (hasPath ? 4 : 3), getInsertStatement(keyspace, table));

    // This statement makes sure there is a DOCROOT. the writetime will always represent the time of
    // latest write to the document.
    statements[statements.length - 3] = getRootDocInsertStatement(keyspace, table, false);
    statements[statements.length - 2] =
        getSubpathArrayDeleteStatement(keyspace, table, pathToDelete);
    statements[statements.length - 1] =
        getPathKeysDeleteStatement(keyspace, table, pathToDelete, patchedKeys);

    if (hasPath) {
      // Only deleting the root path when there is a defined `pathToDelete` ensures that the DOCROOT
      // entry is never deleted out.
      statements[statements.length - 4] =
          getExactPathDeleteStatement(keyspace, table, pathToDelete);
      vars.add(deleteVarsExact);
    }
    vars.add(rootDocInsertVars);
    vars.add(deleteVarsExact);
    vars.add(deleteVarsWithPathKeys);
    dataStore
        .processBatch(Arrays.asList(statements), vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM))
        .join();
  }

  /**
   * Performs an insert using the @param vars provided, all in one batch, using `IF NOT EXISTS`
   * behavior on the `key`.
   *
   * @return true if data was written, false otherwise
   */
  public boolean insertBatchIfNotExists(
      String keyspace, String table, String key, List<Object[]> vars) {
    PreparedStatement[] statements = new PreparedStatement[vars.size() + 1];
    statements[0] = getRootDocInsertStatement(keyspace, table, true);
    Object[] rootInsertVars = new Object[1];
    rootInsertVars[0] = key;

    vars.add(0, rootInsertVars);
    Arrays.fill(statements, 1, statements.length, getInsertStatement(keyspace, table));

    ResultSet res =
        dataStore
            .processBatch(
                Arrays.asList(statements), vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM))
            .join();

    Column appliedCol = Column.reference("[applied]");
    List<Row> rows = res.rows();

    return rows.stream().allMatch(r -> r.getBoolean(appliedCol));
  }

  public void delete(
      String keyspace, String table, String key, List<String> pathToDelete, long microsSinceEpoch) {
    PreparedStatement[] statements = new PreparedStatement[1];
    statements[0] = getPrefixDeleteStatement(keyspace, table, pathToDelete);
    Object[] deleteVars = new Object[pathToDelete.size() + 2];
    deleteVars[0] = microsSinceEpoch;
    deleteVars[1] = key;
    for (int i = 2; i < deleteVars.length; i++) {
      deleteVars[i] = pathToDelete.get(i - 2);
    }

    List<Object[]> vars = new ArrayList<>();
    vars.add(deleteVars);
    dataStore
        .processBatch(Arrays.asList(statements), vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM))
        .join();
  }

  public void deleteDeadLeaves(
      String keyspaceName, String tableName, String key, Map<String, List<JsonNode>> deadLeaves) {
    Long now = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
    List<PreparedStatement> statements = new ArrayList<>();
    List<Object[]> vars = new ArrayList<>();
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

      Object[] deleteVars = new Object[pathToDelete.length + keysToDelete.size() + 2];
      Object[] arrayDeleteVars = new Object[pathToDelete.length + 2];
      deleteVars[0] = now;
      deleteVars[1] = key;
      arrayDeleteVars[0] = now;
      arrayDeleteVars[1] = key;
      for (int i = 0; i < pathToDelete.length; i++) {
        deleteVars[i + 2] = pathToDelete[i];
        arrayDeleteVars[i + 2] = pathToDelete[i];
      }

      for (int i = 0; i < keysToDelete.size(); i++) {
        deleteVars[i + 2 + pathToDelete.length] = keysToDelete.get(i);
      }

      if (!keysToDelete.isEmpty()) {
        statements.add(
            getPathKeysDeleteStatement(
                keyspaceName, tableName, Arrays.asList(pathToDelete), keysToDelete));
        vars.add(deleteVars);
      }

      if (deleteArray) {
        statements.add(
            getSubpathArrayDeleteStatement(keyspaceName, tableName, Arrays.asList(pathToDelete)));
        vars.add(arrayDeleteVars);
      }
    }

    // Fire this off in a future
    dataStore.processBatch(statements, vars, Optional.of(ConsistencyLevel.LOCAL_QUORUM));
  }

  public Map<String, Object> newBindMap(List<String> path, long microsSinceEpoch) {
    Map<String, Object> bindMap = new LinkedHashMap<>(MAX_DEPTH + 7);

    bindMap.put("key", Parameter.UNSET);

    for (int i = 0; i < MAX_DEPTH; i++) {
      String value = "";
      if (i < path.size()) {
        value = path.get(i);
      }
      bindMap.put("p" + i, value);
    }

    bindMap.put("leaf", Parameter.UNSET);
    bindMap.put("text_value", Parameter.UNSET);
    bindMap.put("dbl_value", Parameter.UNSET);
    bindMap.put("bool_value", Parameter.UNSET);
    bindMap.put("timestamp", microsSinceEpoch);

    return bindMap;
  }
}
