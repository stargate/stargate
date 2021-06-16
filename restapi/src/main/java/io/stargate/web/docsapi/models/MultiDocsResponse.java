package io.stargate.web.docsapi.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

public class MultiDocsResponse {
  @JsonProperty("documentIds")
  List<String> documentIds;

  @ApiModelProperty(value = "The ids of the documents created, in order of creation")
  public List<String> getDocumentIds() {
    return documentIds;
  }

  public MultiDocsResponse setDocumentIds(List<String> documentIds) {
    this.documentIds = documentIds;
    return this;
  }

  @JsonCreator
  public MultiDocsResponse(@JsonProperty("documentIds") final List<String> documentIds) {
    this.documentIds = documentIds;
  }
}
