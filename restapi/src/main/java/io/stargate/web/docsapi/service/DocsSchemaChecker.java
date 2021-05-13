package io.stargate.web.docsapi.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import java.util.Objects;

/**
 * A helper class for checking that particular keyspace/table combinations exist and are valid
 * document API collections.
 */
public class DocsSchemaChecker {
  public static class KeyspaceAndTable {
    private String keyspace;
    private String table;

    public KeyspaceAndTable(String keyspace, String table) {
      this.keyspace = keyspace;
      this.table = table;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      KeyspaceAndTable that = (KeyspaceAndTable) o;
      return keyspace.equals(that.keyspace) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
      return Objects.hash(keyspace, table);
    }
  }

  private final Cache<KeyspaceAndTable, Boolean> validatedDocCollections =
      Caffeine.newBuilder().maximumSize(10_000).build();

  public void checkTableExists(String keyspace, String table, DocumentDB db) {
    db.tableExists(keyspace, table);
  }

  public boolean isDocumentsTable(String keyspace, String table, DocumentDB db) {
    KeyspaceAndTable keyspaceAndTable = new KeyspaceAndTable(keyspace, table);
    Boolean valid = validatedDocCollections.getIfPresent(keyspaceAndTable);
    if (valid == null) {
      valid = db.isDocumentsTable(keyspace, table);
      validatedDocCollections.put(keyspaceAndTable, valid);
    }
    return valid;
  }

  public void checkValidity(String keyspace, String table, DocumentDB db) {
    checkTableExists(keyspace, table, db);
    boolean isDocsTable = isDocumentsTable(keyspace, table, db);
    if (!isDocsTable) {
      throw new ErrorCodeRuntimeException(
          ErrorCode.DOCS_API_TABLE_NOT_A_COLLECTION,
          String.format(
              "The Cassandra table %s.%s is not a Documents collection. Accessing arbitrary tables via the Documents API is not permitted.",
              keyspace, table));
    }
  }
}
