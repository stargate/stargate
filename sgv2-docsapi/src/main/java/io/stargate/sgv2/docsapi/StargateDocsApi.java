package io.stargate.sgv2.docsapi;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import javax.ws.rs.core.Application;
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
    components =
        @Components(

            // security schemes
            securitySchemes = {
              @SecurityScheme(
                  securitySchemeName = OpenApiConstants.SecuritySchemes.TOKEN,
                  type = SecuritySchemeType.APIKEY,
                  in = SecuritySchemeIn.HEADER,
                  apiKeyName = Constants.AUTHENTICATION_TOKEN_HEADER_NAME)
            },

            // reusable parameters
            parameters = {
              @Parameter(
                  in = ParameterIn.PATH,
                  name = OpenApiConstants.Parameters.NAMESPACE,
                  required = true,
                  schema = @Schema(implementation = String.class, pattern = "\\w+"),
                  description = "The namespace where the collection is located.",
                  example = "cycling"),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = OpenApiConstants.Parameters.COLLECTION,
                  required = true,
                  schema = @Schema(implementation = String.class, pattern = "\\w+"),
                  description = "The name of the collection.",
                  example = "events"),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.RAW,
                  description = "Unwrap results.",
                  schema = @Schema(implementation = boolean.class)),
            },

            // reusable examples
            examples = {
              @ExampleObject(
                  name = OpenApiConstants.Examples.GENERAL_BAD_REQUEST,
                  value =
                      """
                      {
                          "code": 400,
                          "description": "Request invalid: payload not provided."
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.GENERAL_UNAUTHORIZED,
                  value =
                      """
                      {
                          "code": 401,
                          "description": "Unauthorized operation.",
                          "grpcStatus": "PERMISSION_DENIED"
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.GENERAL_MISSING_TOKEN,
                  value =
                      """
                      {
                          "code": 401,
                          "description": "Missing token."
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.GENERAL_SERVER_SIDE_ERROR,
                  value =
                      """
                      {
                          "code": 500,
                          "description": "Internal server error."
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.GENERAL_SERVICE_UNAVAILABLE,
                  value =
                      """
                      {
                          "code": 503,
                          "description": "gRPC service unavailable (UNAVAILABLE->Service Unavailable): UNAVAILABLE: io exception",
                          "grpcStatus": "UNAVAILABLE"
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.NAMESPACE_DOES_NOT_EXIST,
                  value =
                      """
                      {
                          "code": 404,
                          "description": "Unknown namespace cycling, you must create it first."
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.COLLECTION_DOES_NOT_EXIST,
                  value =
                      """
                      {
                          "code": 404,
                          "description": "Collection 'events' not found."
                      }
                      """),
            },

            // reusable response
            responses = {
              @APIResponse(
                  name = OpenApiConstants.Responses.GENERAL_400,
                  responseCode = "400",
                  description = "Bad request.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(ref = OpenApiConstants.Examples.GENERAL_BAD_REQUEST)
                          },
                          schema =
                              @org.eclipse.microprofile.openapi.annotations.media.Schema(
                                  implementation = ApiError.class))),
              @APIResponse(
                  name = OpenApiConstants.Responses.GENERAL_401,
                  responseCode = "401",
                  description = "Unauthorized.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(ref = OpenApiConstants.Examples.GENERAL_UNAUTHORIZED),
                            @ExampleObject(ref = OpenApiConstants.Examples.GENERAL_MISSING_TOKEN),
                          },
                          schema =
                              @org.eclipse.microprofile.openapi.annotations.media.Schema(
                                  implementation = ApiError.class))),
              @APIResponse(
                  name = OpenApiConstants.Responses.GENERAL_500,
                  responseCode = "500",
                  description = "Unexpected server-side error.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(
                                ref = OpenApiConstants.Examples.GENERAL_SERVER_SIDE_ERROR),
                          },
                          schema =
                              @org.eclipse.microprofile.openapi.annotations.media.Schema(
                                  implementation = ApiError.class))),
              @APIResponse(
                  name = OpenApiConstants.Responses.GENERAL_503,
                  responseCode = "503",
                  description = "Data store service is not available.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(
                                ref = OpenApiConstants.Examples.GENERAL_SERVICE_UNAVAILABLE),
                          },
                          schema =
                              @org.eclipse.microprofile.openapi.annotations.media.Schema(
                                  implementation = ApiError.class))),
            }),
    tags = {
      @Tag(
          name = OpenApiConstants.Tags.COLLECTIONS,
          description = "Collection management operations.")
    })
public class StargateDocsApi extends Application {}
