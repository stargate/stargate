package io.stargate.bridge.service.docsapi;

import io.stargate.db.datastore.Row;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Accumulator {

  public static final Accumulator TERM = new Accumulator();

  private final String id;
  private final Comparator<DocProperty> rowComparator;
  private final List<String> docKey;
  private final List<DocProperty> rows;
  private final List<PagingStateSupplier> pagingState;
  private final boolean complete;
  private final Accumulator next;

  public Accumulator() {
    id = null;
    rowComparator =
        Comparator.comparing(
            r -> {
              throw new IllegalStateException("Cannot append to the terminal element");
            });

    docKey = Collections.emptyList();
    rows = Collections.emptyList();
    pagingState = Collections.emptyList();
    next = null;
    complete = false;
  }

  public Accumulator(
      String id, Comparator<DocProperty> rowComparator, List<String> docKey, DocProperty seedRow) {
    this.id = id;
    this.rowComparator = rowComparator;
    this.docKey = docKey;
    this.rows = new ArrayList<>();
    this.pagingState = Collections.emptyList();
    this.next = null;
    this.complete = false;

    rows.add(seedRow);
  }

  public Accumulator(
      Accumulator complete,
      List<DocProperty> rows,
      List<PagingStateSupplier> pagingState,
      Accumulator next) {
    this.id = complete.id;
    this.rowComparator = complete.rowComparator;
    this.docKey = complete.docKey;
    this.rows = rows;
    this.pagingState = pagingState;
    this.next = next;
    this.complete = true;
  }

  boolean isComplete() {
    return complete;
  }

  public RawDocument toDoc() {
    if (!complete) {
      throw new IllegalStateException("Incomplete document.");
    }

    List<Row> docRows = this.rows.stream().map(DocProperty::row).collect(Collectors.toList());

    CombinedPagingState combinedPagingState = new CombinedPagingState(pagingState);

    return new RawDocument(id, docKey, combinedPagingState, docRows);
  }

  private Accumulator complete(PagingStateTracker tracker, Accumulator next) {
    // Deduplicate included rows and run them through the paging state tracker.
    // Note: the `rows` should already be in the order consistent with `rowComparator`.
    List<DocProperty> finalRows = new ArrayList<>(rows.size());
    DocProperty last = null;
    for (DocProperty row : rows) {
      tracker.track(row);

      if (last == null || rowComparator.compare(last, row) != 0) {
        finalRows.add(row);
      }

      last = row;
    }

    List<PagingStateSupplier> currentPagingState = tracker.slice();

    return new Accumulator(this, finalRows, currentPagingState, next);
  }

  private Accumulator end(PagingStateTracker tracker) {
    if (next != null) {
      if (!complete) {
        throw new IllegalStateException("Ending an incomplete document");
      }

      return next.end(tracker);
    }

    if (complete) {
      throw new IllegalStateException("Already complete");
    }

    return complete(tracker, null);
  }

  private void append(Accumulator other) {
    rows.addAll(other.rows);
  }

  public Accumulator combine(PagingStateTracker tracker, Accumulator buffer) {
    if (buffer == TERM) {
      return end(tracker);
    }

    if (complete) {
      if (next == null) {
        throw new IllegalStateException(
            "Unexpected continuation after a terminal document element.");
      }

      return next.combine(tracker, buffer);
    }

    if (docKey.equals(buffer.docKey)) {
      append(buffer);
      return this; // still not complete
    } else {
      return complete(tracker, buffer);
    }
  }
}
