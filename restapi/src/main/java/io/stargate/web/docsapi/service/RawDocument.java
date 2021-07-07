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
package io.stargate.web.docsapi.service;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.datastore.Row;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class RawDocument {

  private final String id;
  private final List<String> docKey;
  private final Function<ResumeMode, ByteBuffer> pagingState;
  private final List<Row> rows;

  public RawDocument(
      String id,
      List<String> docKey,
      Function<ResumeMode, ByteBuffer> pagingState,
      List<Row> rows) {
    this.id = id;
    this.docKey = docKey;
    this.pagingState = pagingState;
    this.rows = rows;
  }

  public String id() {
    return id;
  }

  public List<String> key() {
    return docKey;
  }

  public List<Row> rows() {
    return rows;
  }

  public Single<RawDocument> populateFrom(Flowable<RawDocument> docs) {
    return docs.take(1)
        .singleElement()
        .map(this::populateFrom)
        .switchIfEmpty(Single.fromCallable(() -> replaceRows(Collections.emptyList())));
  }

  private RawDocument replaceRows(List<Row> newRows) {
    return new RawDocument(id, docKey, pagingState, newRows);
  }

  public RawDocument populateFrom(RawDocument doc) {
    if (!id.equals(doc.id)) {
      throw new IllegalStateException(
          String.format("Document ID mismatch. Expecting %s, got %s", id, doc.id));
    }

    // Use query state of current doc, but rows from the other doc
    return replaceRows(doc.rows);
  }

  public boolean hasPagingState() {
    return makePagingState() != null;
  }

  public ByteBuffer makePagingState() {
    ResumeMode resumeMode;
    if (docKey.size() > 1) {
      resumeMode = ResumeMode.NEXT_ROW;
    } else {
      resumeMode = ResumeMode.NEXT_PARTITION;
    }

    return pagingState.apply(resumeMode);
  }
}
