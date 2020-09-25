package io.stargate.db.datastore;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.Statement;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

class PersistenceBackedResultSet implements ResultSet {
  private final Persistence.Connection connection;
  private final Parameters parameters;
  private final Statement statement;
  private final ProtocolVersion driverProtocolVersion;
  private final Deque<Row> fetchedRows;
  private final List<Column> columns;

  // Paging state to fetch the next page, or null is we've fetched all pages.
  private ByteBuffer nextPagingState;

  PersistenceBackedResultSet(
      Persistence.Connection connection,
      Parameters parameters,
      Statement statement,
      ProtocolVersion driverProtocolVersion,
      Result.Rows initialPage) {
    this.connection = connection;
    // We get our metadata in our initial page; let's skip it for following pages
    this.parameters = parameters.withoutMetadataInResult();
    this.statement = statement;
    this.driverProtocolVersion = driverProtocolVersion;
    this.fetchedRows = new ArrayDeque<>(parameters.pageSize().orElse(32));
    this.columns = initialPage.resultMetadata.columns;
    processNewPage(initialPage);
  }

  private void processNewPage(Result.Rows page) {
    for (List<ByteBuffer> rowValues : page.rows) {
      fetchedRows.addLast(new ArrayListBackedRow(columns, rowValues, driverProtocolVersion));
    }
    nextPagingState = page.resultMetadata.pagingState;
  }

  private void fetchNextPage() {
    try {
      // Note: we could have add a timeout on that get() for security. That said, we don't want
      // to pull a random number, and adding a new config for that should probably be discussed.
      // But it's probably good enough to rely on the persistence layer query timeout.
      Result result =
          connection
              .execute(statement, parameters.withPagingState(nextPagingState), System.nanoTime())
              .get();

      switch (result.kind) {
        case Void:
          nextPagingState = null;
          break;
        case Rows:
          processNewPage((Result.Rows) result);
          break;
        default:
          throw new IllegalStateException(
              String.format("Unexpected %s result received for a result set page", result.kind));
      }
    } catch (InterruptedException e) {
      // We don't play with interruptions, so hopefully this never happen
      throw new RuntimeException("Interrupted while waiting on new page results");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      // We only use unchecked exceptions, and we'd rather not wrap them as it makes it harder to
      // work with.
      throw (cause instanceof RuntimeException)
          ? ((RuntimeException) cause)
          : new RuntimeException(cause);
    }
  }

  private boolean hasNextRow() {
    while (true) {
      if (!fetchedRows.isEmpty()) {
        return true;
      }
      if (nextPagingState == null) {
        return false;
      }
      fetchNextPage();
    }
  }

  private Row nextRow() {
    while (true) {
      Row nextRow = fetchedRows.pollFirst();
      if (nextRow != null) {
        return nextRow;
      }
      if (nextPagingState == null) {
        throw new NoSuchElementException();
      }
      fetchNextPage();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return hasNextRow();
      }

      @Override
      public Row next() {
        return nextRow();
      }
    };
  }

  @Override
  public Row one() {
    return nextRow();
  }

  @Override
  public List<Row> rows() {
    List<Row> all = new ArrayList<>();
    while (hasNextRow()) {
      all.add(nextRow());
    }
    return all;
  }

  @Override
  public List<Row> currentPageRows() {
    List<Row> fetched = new ArrayList<>();
    while (!fetchedRows.isEmpty()) {
      fetched.add(fetchedRows.pollFirst());
    }
    return fetched;
  }

  @Override
  public boolean hasNoMoreFetchedRows() {
    return fetchedRows.isEmpty();
  }

  @Override
  public ByteBuffer getPagingState() {
    return nextPagingState;
  }

  @Override
  public boolean waitedForSchemaAgreement() {
    return false;
  }
}
