/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.web.docsapi.exception;

import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.service.DocsApiConfiguration;
import io.stargate.web.models.Error;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/** Simple enumeration for the error code that can provide back the correct response to the user. */
public enum ErrorCode {

  /** Generic data store errors. */
  DATASTORE_KEYSPACE_DOES_NOT_EXIST(Response.Status.NOT_FOUND, "An unknown namespace provided."),

  DATASTORE_TABLE_DOES_NOT_EXIST(Response.Status.NOT_FOUND, "An unknown table provided."),

  DATASTORE_TABLE_NAME_INVALID(
      Response.Status.BAD_REQUEST,
      "The table name contains invalid characters. Valid characters are alphanumeric and underscores."),

  /** Document API. */
  DOCS_API_TABLE_NOT_A_COLLECTION(
      Response.Status.BAD_REQUEST,
      "The requested database table is not a Documents API Collection."),
  DOCS_API_GENERAL_ARRAY_LENGTH_EXCEEDED(
      Response.Status.BAD_REQUEST,
      String.format(
          "Max array length of %s exceeded.", DocsApiConfiguration.DEFAULT.getMaxArrayLength())),

  DOCS_API_GENERAL_DEPTH_EXCEEDED(
      Response.Status.BAD_REQUEST,
      String.format("Max depth of %s exceeded.", DocsApiConfiguration.DEFAULT.getMaxDepth())),

  DOCS_API_GENERAL_FIELDS_INVALID(
      Response.Status.BAD_REQUEST,
      "The `fields` must be a JSON array and each field must be a non-empty string."),

  DOCS_API_GENERAL_INVALID_FIELD_NAME(
      Response.Status.BAD_REQUEST,
      String.format(
          "The characters %s are not permitted in JSON field names.",
          DocumentDB.getForbiddenCharactersMessage())),

  DOCS_API_GENERAL_PAGE_SIZE_EXCEEDED(
      Response.Status.BAD_REQUEST,
      String.format(
          "The parameter `page-size` is limited to %d.",
          DocsApiConfiguration.DEFAULT.getMaxPageSize())),

  DOCS_API_GET_FIELDS_WITHOUT_WHERE(
      Response.Status.BAD_REQUEST, "Selecting fields is not allowed without `where`."),

  DOCS_API_GET_MULTIPLE_FIELD_CONDITIONS(
      Response.Status.BAD_REQUEST, "Conditions across multiple fields are not yet supported."),

  DOCS_API_GET_CONDITION_FIELDS_NOT_REFERENCED(
      Response.Status.BAD_REQUEST,
      "When selecting `fields`, the field referenced by `where` must be in the selection."),

  DOCS_API_PATCH_ARRAY_NOT_ACCEPTED(
      Response.Status.BAD_REQUEST,
      "A patch operation must be done with a JSON object, not an array."),

  DOCS_API_PUT_PAYLOAD_INVALID(
      Response.Status.BAD_REQUEST, "A put operation failed due to the invalid payload."),

  DOCS_API_SEARCH_FILTER_INVALID(
      Response.Status.BAD_REQUEST, "A filter operation and value resolved as invalid."),

  DOCS_API_SEARCH_OBJECT_REQUIRED(
      Response.Status.BAD_REQUEST, "Search was expecting a JSON object as input."),

  DOCS_API_SEARCH_RESULTS_NOT_FITTING(
      Response.Status.BAD_REQUEST,
      "The results as requested must fit in one page, try increasing the `page-size` parameter.");

  /** Status of the response. */
  private final Response.Status responseStatus;

  /**
   * Default message of this error code. Used when no custom message is passed to the {@link
   * #toResponseBuilder(String)}.
   */
  private final String defaultMessage;

  ErrorCode(Response.Status responseStatus, String defaultMessage) {
    this.responseStatus = responseStatus;
    this.defaultMessage = defaultMessage;
  }

  /** @return Returns {@link Response} using the ErrorCode default message. */
  public Response toResponse() {
    return toResponse(defaultMessage);
  }

  /**
   * @param message message or <code>null</code> to use the default error code message
   * @return Returns {@link Response} using the custom message.
   */
  public Response toResponse(String message) {
    return toResponseBuilder(message).build();
  }

  /**
   * @param message message or <code>null</code> to use the default error code message
   * @return Returns the Response.ResponseBuilder in order to be able to alter the final response to
   *     the user
   */
  public Response.ResponseBuilder toResponseBuilder(String message) {
    Error error = new Error(message, responseStatus.getStatusCode());

    // declare as MediaType.APPLICATION_JSON_TYPE as we have non-string entity here
    return Response.status(responseStatus).type(MediaType.APPLICATION_JSON_TYPE).entity(error);
  }

  public String getDefaultMessage() {
    return defaultMessage;
  }
}
