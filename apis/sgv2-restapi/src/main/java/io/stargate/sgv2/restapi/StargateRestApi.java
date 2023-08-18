package io.stargate.sgv2.restapi;

import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.restapi.config.constants.RestOpenApiConstants;
import jakarta.ws.rs.core.Application;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
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
                  name = RestOpenApiConstants.Parameters.FIELDS,
                  description = "Comma delimited list of keys to include",
                  example = "name,title",
                  required = false,
                  schema = @Schema(type = SchemaType.STRING)),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = RestOpenApiConstants.Parameters.KEYSPACE_NAME,
                  description = "Name of the keyspace to use for the request",
                  example = "cycling",
                  required = true,
                  schema = @Schema(type = SchemaType.STRING)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.KEYSPACE_AS_QUERY_PARAM,
                  description = "Name of the keyspace to use for the request",
                  example = "cycling",
                  required = false,
                  schema = @Schema(type = SchemaType.STRING)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.PAGE_SIZE,
                  description = "Restrict the number of returned items",
                  example = "10",
                  required = false,
                  schema = @Schema(implementation = Integer.class)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.PAGE_STATE,
                  description = "Cassandra page state, used for pagination on consecutive requests",
                  required = false,
                  schema = @Schema(type = SchemaType.STRING)),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = RestOpenApiConstants.Parameters.PRIMARY_KEY,
                  description =
                      "Value from the primary key column for the table. Define composite keys by separating values"
                          + " with slashes (`val1/val2...`) in the order they were defined. </br>"
                          + "For example, if the composite key was defined as `PRIMARY KEY(race_year, race_name)`"
                          + " then the primary key in the path would be `race_year/race_name` ",
                  required = true,
                  schema = @Schema(implementation = String.class, type = SchemaType.ARRAY)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.RAW,
                  description = "Whether to 'unwrap' results object (omit wrapper)",
                  required = false,
                  schema = @Schema(implementation = boolean.class)),
             @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.COMPACT_MAP_DATA,
                  description = "Whether to return/expect the map data in compact format",
                  required = false,
                  schema = @Schema(implementation = boolean.class)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = RestOpenApiConstants.Parameters.SORT,
                  description =
                      "JSON Object with key(s) to sort results by as keys and 'ASC' or 'DESC' as value",
                  required = false,
                  schema = @Schema(type = SchemaType.OBJECT),
                  examples = {
                    @ExampleObject(
                        name = "sort.single",
                        summary = "Single sorting column",
                        value =
                            """
                            {"count": "DESC"}}
                            """)
                  }),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = RestOpenApiConstants.Parameters.TABLE_NAME,
                  description = "Name of the table to use for the request",
                  required = true,
                  example = "cycling_events",
                  schema = @Schema(type = SchemaType.STRING)),
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
                  name = RestOpenApiConstants.Responses.GENERAL_204,
                  responseCode = "204",
                  description = "No content"),
              @APIResponse(
                  name = RestOpenApiConstants.Responses.GENERAL_400,
                  responseCode = "400",
                  description = "Bad request",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          examples = {
                            @ExampleObject(ref = RestOpenApiConstants.Examples.GENERAL_BAD_REQUEST),
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
                  name = RestOpenApiConstants.Responses.GENERAL_404,
                  responseCode = "404",
                  description = "Not Found",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
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
public class StargateRestApi extends Application {}
