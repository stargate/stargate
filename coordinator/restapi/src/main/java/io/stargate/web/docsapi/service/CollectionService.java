package io.stargate.web.docsapi.service;

import io.stargate.db.schema.Index;
import io.stargate.db.schema.SecondaryIndex;
import io.stargate.db.schema.Table;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.CollectionUpgradeType;
import io.stargate.web.docsapi.models.DocCollection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CollectionService {
  public DocCollection getCollectionInfo(Table table, DocumentDB db) {
    if (db.supportsSAI()) {
      List<Index> indexes = table.indexes();
      // If all secondary indexes are not SAI or there are no secondary indexes,
      // then an upgrade is available.
      List<SecondaryIndex> secondaryIndexes =
          indexes.stream()
              .filter(i -> i instanceof SecondaryIndex)
              .map(i -> (SecondaryIndex) i)
              .collect(Collectors.toList());
      boolean upgradeAvailable =
          secondaryIndexes.size() == 0 || secondaryIndexes.stream().allMatch(i -> !i.isCustom());
      return new DocCollection(
          table.name(),
          upgradeAvailable,
          upgradeAvailable ? CollectionUpgradeType.SAI_INDEX_UPGRADE : null);
    } else {
      return new DocCollection(table.name(), false, null);
    }
  }

  public boolean createCollection(String keyspaceName, String tableName, DocumentDB docDB) {
    boolean created = docDB.maybeCreateTable(keyspaceName, tableName);
    if (!created) {
      return false;
    }
    docDB.maybeCreateTableIndexes(keyspaceName, tableName);
    return true;
  }

  public void deleteCollection(String keyspaceName, String tableName, DocumentDB docDB)
      throws InterruptedException, ExecutionException {
    docDB.deleteTable(keyspaceName, tableName);
  }

  public boolean upgradeCollection(
      String keyspaceName, String tableName, DocumentDB docDB, CollectionUpgradeType upgradeType) {
    if (upgradeType == CollectionUpgradeType.SAI_INDEX_UPGRADE) {
      return docDB.upgradeTableIndexes(keyspaceName, tableName);
    }
    String msg = String.format("Invalid upgrade type: %s.", upgradeType);
    throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_GENERAL_UPGRADE_INVALID, msg);
  }
}
