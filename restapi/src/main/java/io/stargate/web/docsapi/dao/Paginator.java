package io.stargate.web.docsapi.dao;

import io.stargate.web.docsapi.service.RawDocument;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

/**
 * This class is in charge of keeping the data of the Docs API pagination process. It's not only a
 * stateful component, but a state machine with a initial state. It's created on the REST layer and
 * passed through to the DocumentService that is in charge of its changes. After being populated its
 * `documentPageState` can be passed by the REST layer to the client as a JSON string.
 */
public class Paginator {

  public final int docPageSize;
  private ByteBuffer currentPageState; // keeps track of the DB page state while querying the DB

  public Paginator(String pageState, int pageSize) {
    if (pageSize <= 0) {
      throw new IllegalArgumentException("Invalid page size: " + pageSize);
    }

    docPageSize = pageSize;

    if (pageState != null) {
      byte[] decodedBytes = Base64.getDecoder().decode(pageState);
      this.currentPageState = ByteBuffer.wrap(decodedBytes);
    }
  }

  /**
   * Utility to make the external paging state from docs, without side effects in {@link Paginator}.
   *
   * @param paginator Paginator
   * @param docs document
   * @return String to serve to external
   */
  public static String makeExternalPagingState(Paginator paginator, List<RawDocument> docs) {
    // If we have less docs than the page requires, this means there's no point requesting the
    // next page. Note that in this case the last doc in the list _may_ have an internal paging
    // state. This may happen if some docs are filtered in memory after fetching from persistence.
    if (docs.size() >= paginator.docPageSize) {
      RawDocument lastDoc = docs.get(docs.size() - 1);
      ByteBuffer byteBuffer = lastDoc.makePagingState();
      if (null != byteBuffer) {
        return Base64.getEncoder().encodeToString(byteBuffer.array());
      }
    }
    return null;
  }

  public String makeExternalPagingState() {
    if (currentPageState == null) {
      return null;
    }

    return Base64.getEncoder().encodeToString(currentPageState.array());
  }

  public void clearDocumentPageState() {
    this.currentPageState = null;
  }

  public void setDocumentPageState(RawDocument document) {
    this.currentPageState = document.makePagingState();
  }

  public void setDocumentPageState(List<RawDocument> docs) {
    if (docs.size() < docPageSize) {
      // If we have less docs than the page requires, this means there's no point requesting the
      // next page. Note that in this case the last doc in the list _may_ have an internal paging
      // state. This may happen if some docs are filtered in memory after fetching from persistence.
      clearDocumentPageState();
    } else {
      RawDocument lastDoc = docs.get(docs.size() - 1);
      setDocumentPageState(lastDoc);
    }
  }

  public ByteBuffer getCurrentDbPageState() {
    return currentPageState;
  }
}
