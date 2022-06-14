package io.stargate.sgv2.docsapi.api.v2.model.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.stargate.sgv2.docsapi.models.ExecutionProfile;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class MultiDocsResponse {
  @JsonProperty("documentIds")
  List<String> documentIds;

  @JsonProperty("profile")
  ExecutionProfile profile;

  @Schema(description = "The ids of the documents successfully created, in order of creation")
  public List<String> getDocumentIds() {
    return documentIds;
  }

  public MultiDocsResponse setDocumentIds(List<String> documentIds) {
    this.documentIds = documentIds;
    return this;
  }

  @Schema(description = "Profiling information related to the execution of the request (optional)")
  public ExecutionProfile getProfile() {
    return profile;
  }

  @JsonCreator
  public MultiDocsResponse(
      @JsonProperty("documentIds") final List<String> documentIds,
      @JsonProperty("profile") ExecutionProfile profile) {
    this.documentIds = documentIds;
    this.profile = profile;
  }
}
