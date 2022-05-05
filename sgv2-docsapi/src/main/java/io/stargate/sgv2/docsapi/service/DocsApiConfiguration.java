package io.stargate.sgv2.docsapi.service;

/** The configuration for the document API. */
public interface DocsApiConfiguration {

  /** Default config instance */
  DocsApiConfiguration DEFAULT = new DocsApiConfiguration() {};

  int DOCUMENT_SEARCH_PAGE_SIZE = Integer.getInteger("stargate.document_search_page_size", 1000);
  int DOCUMENT_MAX_DEPTH = Integer.getInteger("stargate.document_max_depth", 64);
  int DOCUMENT_MAX_ARRAY_LENGTH = checkMaxArrayLength();
  int MAX_PAGE_SIZE = 20;

  static int checkMaxArrayLength() {
    Integer val = Integer.getInteger("stargate.document_max_array_len", 1000000);
    if (val > 1000000) {
      throw new IllegalStateException(
          "stargate.document_max_array_len cannot be greater than 1000000.");
    }
    return val;
  }

  /** @return Returns max allowed document page size that user can request. */
  default int getMaxPageSize() {
    return MAX_PAGE_SIZE;
  }

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
    return DOCUMENT_SEARCH_PAGE_SIZE;
  }

  /** @return The maximum JSON depth for the documents stored. */
  default int getMaxDepth() {
    return DOCUMENT_MAX_DEPTH;
  }

  /** @return The maximum array length for a single field in the docs API. */
  default int getMaxArrayLength() {
    return DOCUMENT_MAX_ARRAY_LENGTH;
  }
}
