package io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * DTO for the get collections response.
 *
 * @param schema The JSON representing the schema.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JsonSchemaDto(@Schema(description = "The JSON Schema") JsonNode schema) {}
