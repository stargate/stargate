package io.stargate.web.docsapi.dao;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Strings;
import java.nio.ByteBuffer;
import java.util.Base64;

public class DocumentSearchPageState {
  @JsonProperty("documentId")
  String documentId;

  @JsonProperty("internalPageState")
  String internalPageState;

  @JsonCreator
  public DocumentSearchPageState(
      @JsonProperty("documentId") final String documentId,
      @JsonProperty("internalPageState") final String internalPageState) {
    this.documentId = documentId;
    this.internalPageState = internalPageState;
  }

  public DocumentSearchPageState(final String documentId, final ByteBuffer internalPageStateBuf) {
    this.documentId = documentId;
    if (internalPageStateBuf != null) {
      this.internalPageState = Base64.getEncoder().encodeToString(internalPageStateBuf.array());
    }
  }

  @JsonIgnore
  public ByteBuffer getPageState() {
    if (Strings.isNullOrEmpty(internalPageState)) {
      return null;
    }
    return ByteBuffer.wrap(Base64.getDecoder().decode(internalPageState));
  }

  @JsonIgnore
  public String getLastSeenDocId() {
    return documentId;
  }
}
