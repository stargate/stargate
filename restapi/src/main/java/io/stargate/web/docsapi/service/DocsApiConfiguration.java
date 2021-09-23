package io.stargate.web.docsapi.service;

/** The configuration for the document API. */
public interface DocsApiConfiguration {

  /** Default config instance */
  DocsApiConfiguration DEFAULT = new DocsApiConfiguration() {};

  /** @return Returns max allowed document page size that user can request. */
  int MAX_PAGE_SIZE = 20;

  /**
   * @return Returns approximate storage page size to use when querying database, based on the
   *     amount of documents we are searching for. We consider that in average documents have 16
   *     fields.
   */
  default int getApproximateStoragePageSize(int numberOfDocuments) {
    return Math.min(numberOfDocuments * 16, getMaxStoragePageSize());
  }

  /**
   * @return The maximum storage page size that should be allowed when doing any doc API queries.
   */
  default int getMaxStoragePageSize() {
    return Integer.getInteger("stargate.document_search_page_size", 1000);
  }

  /** @return The maximum JSON depth for the documents stored. */
  default int getMaxDepth() {
    return Integer.getInteger("stargate.document_max_depth", 64);
  }

  /** @return The maximum array length for a single field in the docs API. */
  default int getMaxArrayLength() {
    Integer val = Integer.getInteger("stargate.document_max_array_len", 1000000);
    if (val > 1000000) {
      throw new IllegalStateException(
          "stargate.document_max_array_len cannot be greater than 1000000.");
    }
    return val;
  }
}
