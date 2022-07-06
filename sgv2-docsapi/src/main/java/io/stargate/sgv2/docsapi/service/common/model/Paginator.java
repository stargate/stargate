/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.docsapi.service.common.model;

import io.stargate.sgv2.docsapi.service.query.model.RawDocument;
import io.stargate.sgv2.docsapi.service.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is in charge of keeping the data of the Docs API pagination process. It's not only a
 * stateful component, but a state machine with a initial state. It's created on the REST layer and
 * passed through to the DocumentService that is in charge of its changes. After being populated its
 * `documentPageState` can be passed by the REST layer to the client as a JSON string.
 *
 * @author Dmitri Bourlatchkov
 */
public class Paginator {

  public final int docPageSize;
  private ByteBuffer currentPageState; // keeps track of the DB page state while querying the DB

  public Paginator(String pageState, int pageSize) {
    if (pageSize <= 0) {
      throw new IllegalArgumentException("Invalid page size: " + pageSize);
    }

    docPageSize = pageSize;

    if (pageState != null && !pageState.isEmpty()) {
      this.currentPageState = ByteBufferUtils.fromBase64UrlParam(pageState);
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
        return ByteBufferUtils.toBase64ForUrl(byteBuffer);
      }
    }
    return null;
  }

  public String makeExternalPagingState() {
    if (currentPageState == null) {
      return null;
    }

    return ByteBufferUtils.toBase64ForUrl(currentPageState);
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
