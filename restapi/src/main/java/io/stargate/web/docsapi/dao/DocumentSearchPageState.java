package io.stargate.web.docsapi.dao;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.ByteBuffer;
import java.util.Base64;

public class DocumentSearchPageState {
  @JsonProperty("documentId")
  String documentId;

  @JsonProperty("internalPageState")
  String internalPageState;

  @JsonIgnore ByteBuffer internalPageStateBuf;

  @JsonIgnore boolean idFound;

  @JsonCreator
  public DocumentSearchPageState(
      @JsonProperty("documentId") final String documentId,
      @JsonProperty("internalPageState") final String internalPageState) {
    this.documentId = documentId;
    this.internalPageState = internalPageState;
    this.idFound = false;
    if (internalPageState == null) {
      throw new IllegalStateException("Malformed page state: parsed cassandra page state was null");
    }
    this.internalPageStateBuf =
        internalPageState.isEmpty()
            ? null
            : ByteBuffer.wrap(Base64.getDecoder().decode(internalPageState));
  }

  public DocumentSearchPageState(final String documentId, final ByteBuffer internalPageStateBuf) {
    this.documentId = documentId;
    this.internalPageState =
        internalPageStateBuf == null
            ? ""
            : Base64.getEncoder().encodeToString(internalPageStateBuf.array());
    this.internalPageStateBuf = internalPageStateBuf;
    this.idFound = false;
  }

  @JsonIgnore
  public ByteBuffer getPageState() {
    return internalPageStateBuf;
  }

  @JsonIgnore
  public boolean isIdFound() {
    return idFound;
  }

  @JsonIgnore
  public void setIdFound(boolean val) {
    this.idFound = val;
  }

  @JsonIgnore
  public String getLastSeenDocId() {
    return documentId;
  }
}
