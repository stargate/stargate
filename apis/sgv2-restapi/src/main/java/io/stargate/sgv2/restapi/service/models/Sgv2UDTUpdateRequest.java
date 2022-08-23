package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(name = "UserDefinedTypeUpdate")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2UDTUpdateRequest {
  @NotNull private String name;

  @JsonProperty("addFields")
  private List<Sgv2UDT.UDTField> addFields;

  @JsonProperty("renameFields")
  private List<FieldRename> renameFields;

  // For deserializer
  protected Sgv2UDTUpdateRequest() {}

  public Sgv2UDTUpdateRequest(String name) {
    this.name = name;
  }

  @Schema(required = true, description = "User Defined Type name")
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Schema(description = "User Defined Type fields to add")
  public List<Sgv2UDT.UDTField> getAddFields() {
    return addFields;
  }

  public void setAddFields(List<Sgv2UDT.UDTField> addFields) {
    this.addFields = addFields;
  }

  @Schema(description = "User Defined Type fields to rename")
  public List<FieldRename> getRenameFields() {
    return renameFields;
  }

  public void setRenameFields(List<FieldRename> renameFields) {
    this.renameFields = renameFields;
  }

  @Schema(name = "UserDefinedTypeFieldRename")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public static class FieldRename {

    private String from;
    private String to;

    protected FieldRename() {}

    public FieldRename(String from, String to) {
      this.from = from;
      this.to = to;
    }

    @Schema(description = "User Defined Type's old field name")
    public String getFrom() {
      return from;
    }

    public void setFrom(String from) {
      this.from = from;
    }

    @Schema(description = "User Defined Type's new field name")
    public String getTo() {
      return to;
    }

    public void setTo(String to) {
      this.to = to;
    }
  }
}
