package io.stargate.bridge.service.docsapi;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.stargate.db.PagingPosition;
import io.stargate.db.datastore.Row;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class RawDocument {

  private final String id;
  private final List<String> docKey;
  private final PagingStateSupplier pagingState;
  private final List<Row> rows;

  public RawDocument(
      String id, List<String> docKey, PagingStateSupplier pagingState, List<Row> rows) {
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
    PagingPosition.ResumeMode resumeMode;
    if (docKey.size() > 1) {
      resumeMode = PagingPosition.ResumeMode.NEXT_ROW;
    } else {
      resumeMode = PagingPosition.ResumeMode.NEXT_PARTITION;
    }

    return pagingState.makePagingState(resumeMode);
  }
}
