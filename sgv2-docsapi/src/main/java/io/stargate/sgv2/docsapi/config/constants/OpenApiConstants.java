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

package io.stargate.sgv2.docsapi.config.constants;

/** Constants for reusing the OpenAPI objects. */
public interface OpenApiConstants {

  /** Name for the Open API default security scheme. */
  interface SecuritySchemes {

    String TOKEN = "Token";
  }

  interface Tags {
    String NAMESPACES = "Namespaces";
    String COLLECTIONS = "Collections";
    String DOCUMENTS = "Documents";
    String JSON_SCHEMAS = "Json Schemas";
  }

  /** Parameters reference names. */
  interface Parameters {

    String NAMESPACE = "namespace";
    String COLLECTION = "collection";
    String DOCUMENT_ID = "document-id";
    String DOCUMENT_PATH = "document-path";
    String WHERE = "where";
    String FIELDS = "fields";
    String PAGE_SIZE = "page-size";
    String PAGE_STATE = "page-state";
    String PROFILE = "profile";
    String RAW = "raw";
    String ID_PATH = "id-path";
    String TTL = "ttl";
    String TTL_AUTO = "ttl-auto";
  }

  interface Examples {

    // general ones
    String GENERAL_BAD_REQUEST = "Generic bad request";
    String GENERAL_MISSING_TOKEN = "Token missing";
    String GENERAL_UNAUTHORIZED = "Unauthorized";
    String GENERAL_SERVER_SIDE_ERROR = "Server-side error";
    String GENERAL_SERVICE_UNAVAILABLE = "Service unavailable";

    // based on the error code
    String NAMESPACE_DOES_NOT_EXIST = "Namespace does not exist";
    String COLLECTION_DOES_NOT_EXIST = "Collection does not exist";
    String DOCUMENT_DOES_NOT_EXIST = "Document does not exist";
    String DOCUMENT_SINGLE = "Single document";
    String DOCUMENT_SINGLE_UNWRAPPED = "Single document unwrapped";
    String DOCUMENT_SINGLE_WITH_WHERE = "Single document with where condition";
    String SUB_DOCUMENT_SINGLE = "Sub-document";
    String SUB_DOCUMENT_SINGLE_UNWRAPPED = "Sub-document unwrapped";
    String SUB_DOCUMENT_SINGLE_WITH_WHERE = "Sub-document with where condition";
    String SEARCH_DOCUMENTS = "Search documents result";
    String SEARCH_DOCUMENTS_UNWRAPPED = "Search documents result unwrapped";
  }

  /** Response reference names. */
  interface Responses {

    String GENERAL_400 = "GENERAL_400";
    String GENERAL_401 = "GENERAL_401";
    String GENERAL_500 = "GENERAL_500";
    String GENERAL_503 = "GENERAL_503";
  }
}
