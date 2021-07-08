package io.stargate.web.docsapi.service;

import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.service.util.ImmutableKeyspaceAndTable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A helper class for checking that particular keyspace/table combinations exist and are valid
 * document API collections.
 */
public class DocsSchemaChecker {
  private Schema lastCheckedSchema;

  private final ConcurrentHashMap<ImmutableKeyspaceAndTable, Boolean> validatedDocCollections =
      new ConcurrentHashMap<>();

  private void clearCacheOnSchemaChange(DocumentDB db) {
    if (!db.schema().equals(lastCheckedSchema)) {
      validatedDocCollections.clear();
      this.lastCheckedSchema = db.schema();
    }
  }

  public boolean checkValidity(String keyspace, String table, DocumentDB db, boolean throwOnFail) {
    ImmutableKeyspaceAndTable keyspaceAndTable =
        ImmutableKeyspaceAndTable.builder().keyspace(keyspace).table(table).build();
    clearCacheOnSchemaChange(db);
    return validatedDocCollections.computeIfAbsent(
        keyspaceAndTable, ks -> performValidityCheck(ks, db, throwOnFail));
  }

  private boolean performValidityCheck(
      ImmutableKeyspaceAndTable keyspaceAndTable, DocumentDB db, boolean throwOnFail) {
    String keyspace = keyspaceAndTable.getKeyspace();
    String table = keyspaceAndTable.getTable();
    db.tableExists(keyspace, table);
    boolean isDocsTable = db.isDocumentsTable(keyspace, table);
    if (!isDocsTable) {
      if (throwOnFail) {
        throw new ErrorCodeRuntimeException(
            ErrorCode.DOCS_API_GENERAL_TABLE_NOT_A_COLLECTION,
            String.format(
                "The database table %s.%s is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted.",
                keyspace, table));
      } else {
        return false;
      }
    }
    return true;
  }
}
