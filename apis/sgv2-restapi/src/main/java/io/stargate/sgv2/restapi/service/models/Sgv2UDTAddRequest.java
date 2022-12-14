package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import javax.validation.constraints.NotEmpty;

@Schema(name = "UserDefinedTypeAdd")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Sgv2UDTAddRequest(
        @Schema(required = true, description = "User Defined Type name")
        @NotEmpty String name,
        @Schema(required = true, description = "User Defined Type fields")
        @NotEmpty List<Sgv2UDT.UDTField> fields,
        @Schema(
            description =
                "Determines whether to create a new UDT if an UDT with the same name exists. Attempting to create an existing UDT returns an error unless this option is true.")
        boolean ifNotExists) {}
