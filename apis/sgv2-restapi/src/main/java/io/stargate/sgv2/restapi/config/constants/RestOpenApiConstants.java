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

package io.stargate.sgv2.restapi.config.constants;

/** Constants for reusing the OpenAPI objects. */
public interface RestOpenApiConstants {

  /** Name for the Open API default security scheme. */
  interface SecuritySchemes {
    String TOKEN = "Token";
  }

  interface Tags {
    String DATA = "data";
    String SCHEMA = "schemas";
  }

  /** Parameters reference names. */
  interface Parameters {
    String KEYSPACE = "keyspace";
    String RAW = "raw";
    String TABLE_NAME = "table_name";
    String WHERE = "where";
  }

  /** Reused example snippets, mostly for error codes */
  interface Examples {

    // general ones
    String GENERAL_BAD_REQUEST = "Generic bad request";
    String GENERAL_UNAUTHORIZED = "Unauthorized";
    String GENERAL_SERVER_SIDE_ERROR = "Server-side error";
  }

  /** Response reference names. */
  interface Responses {

    String GENERAL_400 = "GENERAL_400";
    String GENERAL_401 = "GENERAL_401";
    String GENERAL_500 = "GENERAL_500";
    String GENERAL_503 = "GENERAL_503";
  }
}
