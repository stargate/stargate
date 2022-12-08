/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi;

import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.exception.model.dto.ApiError;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.documents.model.dto.BuiltInFunctionDto;
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
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
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
                  apiKeyName = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
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
              @Parameter(
                  in = ParameterIn.PATH,
                  name = OpenApiConstants.Parameters.DOCUMENT_PATH,
                  required = true,
                  schema = @Schema(implementation = String.class),
                  description =
                      "The path in the JSON that you want to target. Use path delimiter `/` to target sub-paths, for example to get a JSON object under `$.account.user` use `account/user`. Use `\\` to escape periods, commas, and asterisks."),
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
                  examples = {
                    @ExampleObject(name = "where.empty", summary = "No condition", value = ""),
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
                  description =
                      "A JSON array representing the field names that you want to restrict the results to.",
                  examples = {
                    @ExampleObject(name = "fields.empty", summary = "No fields", value = ""),
                    @ExampleObject(
                        name = "fields.notEmpty",
                        summary = "Fields example",
                        value =
                            """
                            ["race", "location"]
                            """)
                  }),
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
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.TTL,
                  description = "The time-to-live (in seconds) of the document.",
                  schema = @Schema(implementation = int.class)),
              @Parameter(
                  in = ParameterIn.QUERY,
                  name = OpenApiConstants.Parameters.TTL_AUTO,
                  description =
                      "If set to `true` on document update or patch, ensures that the time-to-live for the updated portion of the document will match the parent document. Requires a read-before-write if set to `true`.",
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
                  name = OpenApiConstants.Examples.DOCUMENT_WRITE,
                  value =
                      """
                      {
                          "documentId": "822dc277-9121-4791-8b01-da8154e67d5d"
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.DOCUMENT_WRITE_BATCH,
                  value =
                      """
                      {
                          "documentIds": ["822dc277-9121-4791-8b01-da8154e67d5d, 6334dft4-9153-3642-4f32-da8154e67d5d"]
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
              @ExampleObject(
                  name = OpenApiConstants.Examples.FUNCTION_POP_BODY,
                  value =
                      """
                      {
                          "operation": "$pop"
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.FUNCTION_PUSH_BODY,
                  value =
                      """
                      {
                          "operation": "$push",
                          "value": 123
                      }
                      """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.FUNCTION_SET_BODY,
                  value =
                      """
                    {
                        "operation": "$set",
                        "value": {
                          "a.b.c": "d",
                          "d.e.[2].f": { "g": true }
                        }
                    }
                    """),
              @ExampleObject(
                  name = OpenApiConstants.Examples.FUNCTION_RESPONSE,
                  description =
                      """
                      Note that `data` property content depends on the executed function:

                      * `$pop` - returns the item that was popped from the array
                      * `$push` - returns the complete array, including the item that was pushed
                      """,
                  value =
                      """
                      {
                          "documentId": "822dc277-9121-4791-8b01-da8154e67d5d",
                          "data": 123
                      }
                      """),
            },

            // reusable request bodies
            requestBodies = {
              @RequestBody(
                  name = OpenApiConstants.RequestBodies.PATCH,
                  description = "The JSON of a patch.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          schema = @Schema(type = SchemaType.OBJECT),
                          example =
                              """
                              {
                                  "location": "Berlin"
                              }
                              """)),
              @RequestBody(
                  name = OpenApiConstants.RequestBodies.WRITE,
                  description = "The JSON of a document.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          schema = @Schema(type = SchemaType.OBJECT),
                          example =
                              """
                              {
                                  "location": "London",
                                  "race": {
                                      "competitors": 100,
                                      "start_date": "2022-08-15"
                                  }
                              }
                              """)),
              @RequestBody(
                  name = OpenApiConstants.RequestBodies.WRITE_SUB_DOCUMENT,
                  description = "The JSON of a sub-document.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          schema = @Schema(type = SchemaType.OBJECT),
                          example =
                              """
                              {
                                  "competitors": 100,
                                  "start_date": "2022-08-15"
                              }
                              """)),
              @RequestBody(
                  name = OpenApiConstants.RequestBodies.WRITE_BATCH,
                  description = "The JSON array of a documents for batch write.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          schema = @Schema(type = SchemaType.ARRAY),
                          example =
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
                              """)),
              @RequestBody(
                  name = OpenApiConstants.RequestBodies.FUNCTION,
                  description = "The request body for executing a built-in function.",
                  content =
                      @Content(
                          mediaType = MediaType.APPLICATION_JSON,
                          schema = @Schema(implementation = BuiltInFunctionDto.class),
                          examples = {
                            @ExampleObject(ref = OpenApiConstants.Examples.FUNCTION_POP_BODY),
                            @ExampleObject(ref = OpenApiConstants.Examples.FUNCTION_PUSH_BODY)
                          })),
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
                          schema = @Schema(implementation = ApiError.class))),
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
                          schema = @Schema(implementation = ApiError.class))),
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
                          schema = @Schema(implementation = ApiError.class))),
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
                          schema = @Schema(implementation = ApiError.class))),
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
