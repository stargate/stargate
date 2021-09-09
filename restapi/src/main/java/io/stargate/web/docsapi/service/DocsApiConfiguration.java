package io.stargate.web.docsapi.service;

import io.stargate.web.docsapi.dao.DocumentDB;

/** The configuration for the document API. */
public interface DocsApiConfiguration {

  /** Default config instance */
  DocsApiConfiguration DEFAULT = new DocsApiConfiguration() {};

  /** @return Returns max allowed document page size that user can request. */
  default int getMaxPageSize() {
    return DocumentDB.MAX_PAGE_SIZE;
  }

  /**
   * @return Returns approximate storage page size to use when querying database, based on the
   *     amount of documents we are searching for. We consider that in average documents have 16
   *     fields.
   */
  default int getApproximateStoragePageSize(int numberOfDocuments) {
    return Math.min(numberOfDocuments * 16, DocumentDB.MAX_STORAGE_PAGE_SIZE);
  }

  /**
   * @return The maximum storage page size that should be allowed when doing any doc API queries.
   */
  default int getMaxStoragePageSize() {
    return DocumentDB.MAX_STORAGE_PAGE_SIZE;
  }

  /** @return The maximum JSON depth for the documents stored. */
  default int getMaxDepth() {
    return DocumentDB.MAX_DEPTH;
  }

  /** @return The maximum array length for a single field in the docs API. */
  default int getMaxArrayLength() {
    return DocumentDB.MAX_ARRAY_LENGTH;
  }
}
