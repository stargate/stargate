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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.query.model;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import io.stargate.sgv2.docsapi.service.query.model.paging.PagingStateSupplier;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.immutables.value.Value;

/** Raw document constructed from the Bridge response that represents a partial or full document. */
@Value.Immutable
public interface RawDocument {

  /**
   * @return The id of the document.
   */
  @Value.Parameter
  String id();

  /**
   * @return The keys being considered when creating a document.
   */
  @Value.Parameter
  List<String> documentKeys();

  /**
   * @return The page state supplier to use.
   */
  @Value.Parameter
  PagingStateSupplier pagingState();

  /**
   * @return List of rows belonging to this document.
   */
  @Value.Parameter
  List<RowWrapper> rows();

  /**
   * Populate rows from documents stream. Use query state of current doc, but rows from the first
   * other doc.
   *
   * @param doc Stream
   * @return New document
   */
  default Uni<RawDocument> populateFrom(Multi<RawDocument> docs) {
    return docs

        // select first and map to uni
        .select()
        .first()
        .toUni()
        .onItem()
        .transform(
            doc -> {
              if (null != doc) {
                return populateFrom(doc);
              } else {
                return replaceRows(Collections.emptyList());
              }
            });
  }

  /**
   * Populate rows from other document. Use query state of current doc, but rows from the other doc.
   *
   * @param doc Other doc
   * @return New document
   */
  default RawDocument populateFrom(RawDocument doc) {
    if (!id().equals(doc.id())) {
      throw new IllegalStateException(
          String.format("Document ID mismatch. Expecting %s, got %s", id(), doc.id()));
    }

    // Use query state of current doc, but rows from the other doc
    return replaceRows(doc.rows());
  }

  private RawDocument replaceRows(List<RowWrapper> newRows) {
    return ImmutableRawDocument.of(id(), documentKeys(), pagingState(), newRows);
  }

  /**
   * @return If the document has paging state.
   */
  default boolean hasPagingState() {
    return makePagingState() != null;
  }

  /**
   * @return The paging state of the document.
   */
  default ByteBuffer makePagingState() {
    return pagingState().makePagingState();
  }
}
