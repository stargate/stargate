package io.stargate.web.docsapi.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * This class is in charge of keeping the data of the Docs API pagination process. It's not only a
 * stateful component, but a state machine with a initial state. It's created on the REST layer and
 * passed through to the DocumentService that is in charge of its changes. After being populated its
 * `documentPageState` can be passed by the REST layer to the client as a JSON string.
 */
public class Paginator {

  private final ObjectMapper mapper = new ObjectMapper();

  public final int dbPageSize;
  public final int docPageSize;

  private ByteBuffer currentDbPageState; // keeps track of the DB page state while querying the DB
  private DocumentSearchPageState documentPageState = null;

  public Paginator(String pageState, int pageSize, int dbPageSize) throws IOException {
    docPageSize = Math.max(1, pageSize);
    if (docPageSize > DocumentDB.MAX_PAGE_SIZE) {
      throw new DocumentAPIRequestException("The parameter `page-size` is limited to 20.");
    }

    this.dbPageSize = dbPageSize;

    if (pageState != null) {
      byte[] decodedBytes = Base64.getDecoder().decode(pageState);
      documentPageState = mapper.readValue(decodedBytes, DocumentSearchPageState.class);
    }

    this.currentDbPageState = (documentPageState != null) ? documentPageState.getPageState() : null;
  }

  // Some parts of the document API use bytes that correspond directly to cassandra page states,
  // rather than DocumentSearchPageState which is only used for collections search.
  public Paginator(String pageState, int pageSize, int dbPageSize, boolean raw) throws IOException {
    docPageSize = Math.max(1, pageSize);
    if (docPageSize > DocumentDB.MAX_PAGE_SIZE) {
      throw new DocumentAPIRequestException("The parameter `page-size` is limited to 20.");
    }

    this.dbPageSize = dbPageSize;

    if (pageState != null) {
      byte[] decodedBytes = Base64.getDecoder().decode(pageState);
      if (raw) {
        this.currentDbPageState = ByteBuffer.wrap(decodedBytes);
      } else {
        documentPageState = mapper.readValue(decodedBytes, DocumentSearchPageState.class);
        this.currentDbPageState = documentPageState.getPageState();
      }
    } else {
      this.currentDbPageState = null;
    }
  }

  public String getDocumentPageStateAsString() throws JsonProcessingException {
    if (documentPageState != null) {
      byte[] pagingJson = mapper.writeValueAsBytes(documentPageState);
      return Base64.getEncoder().encodeToString(pagingJson);
    }
    return null;
  }

  public void clearDocumentPageState() {
    this.documentPageState = null;
  }

  public void setCurrentDbPageState(ByteBuffer page) {
    this.currentDbPageState = page;
  }

  public boolean hasDbPageState() {
    return currentDbPageState != null;
  }

  public void setDocumentPageState(String lastIdSeen) {
    documentPageState = new DocumentSearchPageState(lastIdSeen, currentDbPageState);
  }

  public ByteBuffer getCurrentDbPageState() {
    return currentDbPageState;
  }

  // Only used to get the internal Cassandra paging state
  public String getCurrentDbPageStateAsString() {
    if (currentDbPageState != null) {
      return Base64.getEncoder().encodeToString(currentDbPageState.array());
    }
    return null;
  }

  public List<Row> maybeSkipRows(List<Row> rows) {
    return skipSeenRows(documentPageState, rows);
  }

  private List<Row> skipSeenRows(DocumentSearchPageState docPagingState, List<Row> rows) {
    if (docPagingState == null) {
      return rows;
    }

    String lastSeenId = docPagingState.getLastSeenDocId();
    if (lastSeenId == null) {
      return rows;
    }

    boolean idFound = docPagingState.isIdFound();
    List<Row> filteredRows = new ArrayList<>();
    for (Row row : rows) {
      String docId = row.getString("key");
      if (docId.equals(lastSeenId)) {
        idFound = true;
        continue;
      }

      if (idFound) {
        filteredRows.add(row);
      }
    }

    docPagingState.setIdFound(idFound);

    return filteredRows;
  }
}
