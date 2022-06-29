package io.stargate.sgv2.docsapi;

import io.stargate.sgv2.docsapi.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import io.stargate.sgv2.docsapi.config.constants.OpenApiConstants;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
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
                  in = ParameterIn.PATH,
                  name = OpenApiConstants.Parameters.DOCUMENT_ID,
                  required = true,
                  schema = @Schema(implementation = String.class),
                  description = "The ID of the document."),

              // TODO there is no correct way to handle this in Swagger
              //  simply the multi-paths can not be defined in a single param
              @Parameter(
                  in = ParameterIn.PATH,
                  name = OpenApiConstants.Parameters.DOCUMENT_PATH,
                  required = true,
                  schema = @Schema(implementation = String.class, type = SchemaType.ARRAY),
                  description =
                      "The path in the JSON that you want to retrieve. Use `\\` to escape periods, commas, and asterisks."),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.WHERE,
                  description =
                      """
                      A JSON blob with search filters:
                      * allowed predicates: `$eq`, `$ne`, `$in`, `$nin`, `$gt`, `$lt`, `$gte`, `$lte`, `$exists`
                      * allowed boolean operators: `$and`, `$or`, `$not`
                      * allowed hints: `$selectivity` (a number between 0.0 and 1.0, less is better), defines conditions that should be search for first
                      * Use `\\` to escape periods, commas, and asterisks
                      """,
                  schema = @Schema(type = SchemaType.OBJECT),
                  examples = {
                    @ExampleObject(
                        name = "where.single",
                        summary = "Single condition",
                        value =
                            """
                              {"location": {"$eq": "London"}}
                              """),
                    @ExampleObject(
                        name = "where.multi",
                        summary = "Multiple conditions (implicit $and)",
                        value =
                            """
                              {
                                "location": {"$eq": "London"},
                                "race.competitors": {"$gt": 10}
                              }
                              """),
                    @ExampleObject(
                        name = "where.and",
                        summary = "And conditions",
                        value =
                            """
                              {
                                  "$and": [
                                      {"location": {"$in": ["London", "Barcelona"]}},
                                      {"race.competitors": {"$gt": 10}}
                                  ]
                              }
                              """),
                    @ExampleObject(
                        name = "where.or",
                        summary = "Or conditions",
                        value =
                            """
                              {
                                  "$or": [
                                      {"location": {"$eq": "London"}},
                                      {"location": {"$eq": "Barcelona"}}
                                  ]
                              }
                              """),
                    @ExampleObject(
                        name = "where.not",
                        summary = "Using negation",
                        value =
                            """
                              {
                                  "$not": {
                                      "$and": [
                                          {"location": {"$eq": "London"}},
                                          {"race.competitors": {"$gt": 10}}
                                      ]
                                  }
                              }
                              """),
                    @ExampleObject(
                        name = "where.selectivity",
                        summary = "With selectivity",
                        value =
                            """
                              {
                                  "$and": [
                                      {"location": {"$eq": "London"}},
                                      {"race.competitors": {"$gt": 10, "$selectivity": 0}}
                                  ]
                              }
                              """),
                  }),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.FIELDS,
                  description = "The field names that you want to restrict the results to.",
                  example = "[race,location]",
                  schema = @Schema(implementation = String.class, type = SchemaType.ARRAY)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.PAGE_SIZE,
                  description = "The max number of results to return.",
                  schema = @Schema(implementation = Integer.class)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.PAGE_STATE,
                  description =
                      "Cassandra page state, used for pagination on consecutive requests.",
                  schema = @Schema(implementation = boolean.class)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.PROFILE,
                  description =
                      "Whether to include query profiling information in the response (advanced).",
                  schema = @Schema(implementation = boolean.class)),
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
              @ExampleObject(
                  name = OpenApiConstants.Examples.DOCUMENT_DOES_NOT_EXIST,
                  value =
                      """
                      {
                          "code": 404,
                          "description": "A document with the id 822dc277-9121-4791-8b01-da8154e67d5d does not exist."
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.SEARCH_DOCUMENTS,
                  value =
                      """
                      {
                          "data": [
                              {
                                  "location": "London",
                                  "race": {
                                      "competitors": 100,
                                      "start_date": "2022-08-15"
                                  }
                              },
                              {
                                  "location": "Barcelona",
                                  "race": {
                                      "competitors": 30,
                                      "start_date": "2022-09-26"
                                  }
                              }
                          ],
                          "pageState": "c29tZS1leGFtcGxlLXN0YXRl"
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.SEARCH_DOCUMENTS_UNWRAPPED,
                  value =
                      """
                      [
                          {
                              "location": "London",
                              "race": {
                                  "competitors": 100,
                                  "start_date": "2022-08-15"
                              }
                          },
                          {
                              "location": "Barcelona",
                              "race": {
                                  "competitors": 30,
                                  "start_date": "2022-09-26"
                              }
                          }
                      ]
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.DOCUMENT_SINGLE,
                  value =
                      """
                      {
                          "documentId": "822dc277-9121-4791-8b01-da8154e67d5d",
                          "data": {
                              "location": "London",
                              "race": {
                                  "competitors": 100,
                                  "start_date": "2022-08-15"
                              }
                          }
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.DOCUMENT_SINGLE_UNWRAPPED,
                  value =
                      """
                      {
                          "location": "London",
                          "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                          }
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.DOCUMENT_SINGLE_WITH_WHERE,
                  description =
                      """
                      Response example when using a condition like `where={"race.competitors": {"$gt": 0} }` on a document. Selects only path to the matched field.
                      """,
                  value =
                      """
                      {
                          "documentId": "822dc277-9121-4791-8b01-da8154e67d5d",
                          "data": [
                              {
                                  "race": {
                                      "competitors": 100
                                  }
                              }
                          ]
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.SUB_DOCUMENT_SINGLE,
                  description =
                      """
                      Response example when using a sub-document path `/race` on a document like:

                      `
                      {
                          "location": "London",
                          "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                          }
                      }
                      `
                      """,
                  value =
                      """
                      {
                          "documentId": "822dc277-9121-4791-8b01-da8154e67d5d",
                          "data": {
                              "race": {
                                  "competitors": 100,
                                  "start_date": "2022-08-15"
                              }
                          }
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.SUB_DOCUMENT_SINGLE_UNWRAPPED,
                  description =
                      """
                      Response example when using a sub-document path `/race` and unwrapping results (`raw=true`) on a document like:

                      `
                      {
                          "location": "London",
                          "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                          }
                      }
                      `
                      """,
                  value =
                      """
                      {
                          "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                          }
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.SUB_DOCUMENT_SINGLE_WITH_WHERE,
                  description =
                      """
                      Response example when using a sub-document path `/race` and a condition like `where={"competitors": {"$gt": 0} }` on a document like:

                      `
                      {
                          "location": "London",
                          "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                          }
                      }
                      `
                      """,
                  value =
                      """
                      {
                          "race": {
                              "competitors": 100
                          }
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
      @Tag(name = OpenApiConstants.Tags.DOCUMENTS, description = "Document related operations."),
      @Tag(
          name = OpenApiConstants.Tags.JSON_SCHEMAS,
          description = "Json schema management operations."),
      @Tag(
          name = OpenApiConstants.Tags.COLLECTIONS,
          description = "Collection management operations."),
      @Tag(
          name = OpenApiConstants.Tags.NAMESPACES,
          description = "Namespace management operations."),
    })
public class StargateDocsApi extends Application {}
