package io.stargate.sgv2.restapi;

import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@OpenAPIDefinition(
    // note that info is defined via the properties
    info = @Info(title = "", version = ""),
    tags = {
      @Tag(name = RestOpenApiConstants.Tags.DATA, description = "DML operations"),
      @Tag(name = RestOpenApiConstants.Tags.SCHEMA, description = "DDL operations"),
    },
    components =
        @Components(

            // security schemes
            securitySchemes = {
              @SecurityScheme(
                  securitySchemeName = RestOpenApiConstants.SecuritySchemes.TOKEN,
                  type = SecuritySchemeType.APIKEY,
                  in = SecuritySchemeIn.HEADER,
                  apiKeyName = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
            },
            // reusable parameters
            parameters = {
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.RAW,
                  description = "Whether to 'unwrap' results object (omit wrapper)",
                  schema = @Schema(implementation = boolean.class)),
            },

            // reusable examples
            examples = {
              @ExampleObject(
                  name = RestOpenApiConstants.Examples.GENERAL_BAD_REQUEST,
                  value =
                      """
                                        {
                                            "code": 400,
                                            "description": "Request invalid: payload not provided."
                                        }
                                        """),
              @ExampleObject(
                  name = RestOpenApiConstants.Examples.GENERAL_UNAUTHORIZED,
                  value =
                      """
                                        {
                                            "code": 401,
                                            "description": "Unauthorized operation.",
                                            "grpcStatus": "PERMISSION_DENIED"
                                        }
                                        """),
              @ExampleObject(
                  name = RestOpenApiConstants.Examples.GENERAL_SERVER_SIDE_ERROR,
                  value =
                      """
                                        {
                                            "code": 500,
                                            "description": "Internal server error."
                                        }
                                        """),
            },

            // reusable response
            responses = {
              @APIResponse(
                  name = RestOpenApiConstants.Responses.GENERAL_400,
                  responseCode = "400",
                  description = "Bad request",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(ref = RestOpenApiConstants.Examples.GENERAL_BAD_REQUEST)
                          },
                          schema = @Schema(implementation = ApiError.class))),
              @APIResponse(
                  name = RestOpenApiConstants.Responses.GENERAL_401,
                  responseCode = "401",
                  description = "Unauthorized",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(
                                ref = RestOpenApiConstants.Examples.GENERAL_UNAUTHORIZED),
                          },
                          schema = @Schema(implementation = ApiError.class))),
              @APIResponse(
                  name = RestOpenApiConstants.Responses.GENERAL_500,
                  responseCode = "500",
                  description = "Internal server error",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(
                                ref = RestOpenApiConstants.Examples.GENERAL_SERVER_SIDE_ERROR),
                          },
                          schema = @Schema(implementation = ApiError.class))),
            }))
public class StargateRestApi {}
