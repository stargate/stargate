package io.stargate.web.docsapi.service;

import io.stargate.web.docsapi.dao.DocumentDB;

public interface DocsApiConfiguration {
  DocsApiConfiguration DEFAULT = new DocsApiConfiguration() {};

  default int getMaxPageSize() {
    return DocumentDB.MAX_PAGE_SIZE;
  }

  default int getStoragePageSize(int numberOfDocuments) {
    return Math.min(numberOfDocuments * 16, DocumentDB.MAX_STORAGE_PAGE_SIZE);
  }

  default int getMaxStoragePageSize() {
    return DocumentDB.MAX_STORAGE_PAGE_SIZE;
  }

  default int getMaxDepth() {
    return DocumentDB.MAX_DEPTH;
  }

  default int getMaxArrayLength() {
    return DocumentDB.MAX_ARRAY_LENGTH;
  }
}
