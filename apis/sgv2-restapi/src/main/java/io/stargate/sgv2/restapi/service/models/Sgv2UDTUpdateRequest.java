package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(name = "UserDefinedTypeUpdate")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Sgv2UDTUpdateRequest(
    @Schema(required = true, description = "User Defined Type name")
        @NotBlank(message = "UserDefinedTypeUpdate.name must be provided")
        String name,
    @Schema(description = "User Defined Type fields to add") List<Sgv2UDT.UDTField> addFields,
    @Schema(description = "User Defined Type fields to rename") List<FieldRename> renameFields) {

  @Schema(name = "UserDefinedTypeFieldRename")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record FieldRename(
      @Schema(description = "User Defined Type's old field name") @NotEmpty String from,
      @Schema(description = "User Defined Type's new field name") @NotEmpty String to) {}
}
