package io.stargate.web.docsapi.dao;

import io.stargate.db.PagingPosition;
import io.stargate.db.PagingPosition.ResumeMode;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.Type;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * This class is in charge of keeping the data of the Docs API pagination process. It's not only a
 * stateful component, but a state machine with a initial state. It's created on the REST layer and
 * passed through to the DocumentService that is in charge of its changes. After being populated its
 * `documentPageState` can be passed by the REST layer to the client as a JSON string.
 */
public class Paginator {

  private final DataStore dataStore;
  public final int dbPageSize;
  public final int docPageSize;

  private ByteBuffer currentDbPageState; // keeps track of the DB page state while querying the DB
  private String lastDocumentId;
  private ResultSet resultSet;

  public Paginator(DataStore dataStore, String pageState, int pageSize, int dbPageSize) {
    this.dataStore = dataStore;
    docPageSize = pageSize;
    if (docPageSize < 1) {
      throw new IllegalStateException("Document page size cannot be less than 1.");
    }

    if (docPageSize > DocumentDB.MAX_PAGE_SIZE) {
      throw new DocumentAPIRequestException("The parameter `page-size` is limited to 20.");
    }

    this.dbPageSize = dbPageSize;
    if (this.dbPageSize < 1) {
      throw new IllegalStateException("Database page size cannot be less than 1.");
    }

    if (pageState != null) {
      byte[] decodedBytes = Base64.getDecoder().decode(pageState);
      this.currentDbPageState = ByteBuffer.wrap(decodedBytes);
    }
  }

  public void useResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.currentDbPageState = resultSet.getPagingState();
  }

  public String makeExternalPagingState() {
    ByteBuffer pagingState;

    if (lastDocumentId != null && resultSet != null) {
      ByteBuffer keyValue = dataStore.valueCodec().encode(Type.Text, lastDocumentId);
      Column keyColumn =
          resultSet.columns().stream()
              .filter(c -> c.name().equals("key"))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("key column not selected"));
      PagingPosition position =
          PagingPosition.builder()
              .putCurrentRow(keyColumn, keyValue)
              .resumeFrom(ResumeMode.NEXT_PARTITION)
              .build();
      pagingState = resultSet.makePagingState(position);
    } else {
      pagingState = currentDbPageState;
    }

    if (pagingState == null) {
      return null;
    }

    return Base64.getEncoder().encodeToString(pagingState.array());
  }

  public void clearDocumentPageState() {
    this.lastDocumentId = null;
  }

  public boolean hasDbPageState() {
    return currentDbPageState != null;
  }

  public void setDocumentPageState(String lastIdSeen) {
    this.lastDocumentId = lastIdSeen;
  }

  public ByteBuffer getCurrentDbPageState() {
    return currentDbPageState;
  }
}
