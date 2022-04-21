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

/** Static constants. */
public interface Constants {

  /** Name for the Open API default security scheme. */
  String OPEN_API_DEFAULT_SECURITY_SCHEME = "Token";

  /** Authentication token header name. */
  String AUTHENTICATION_TOKEN_HEADER_NAME = "X-Cassandra-Token";

  /** Tenant identifier header name. */
  String TENANT_ID_HEADER_NAME = "X-Tenant-Id";

  /** Default name of the key column in the document table. */
  String KEY_COLUMN_NAME = "key";

  /** Default name of the leaf column in the document table. */
  String LEAF_COLUMN_NAME = "leaf";

  /** Default name of the string value column in the document table. */
  String STRING_VALUE_COLUMN_NAME = "text_value";

  /** Default name of the double column in the document table. */
  String DOUBLE_VALUE_COLUMN_NAME = "dbl_value";

  /** Default name of the boolean value column in the document table. */
  String BOOLEAN_VALUE_COLUMN_NAME = "bool_value";

  /** The default prefix of the columns that store JSON path. */
  String PATH_COLUMN_PREFIX = "p";

  /** Search token that matches all values in a path. */
  String GLOB_VALUE = "*";

  /** Search token that matches all array elements in a path. */
  String GLOB_ARRAY_VALUE = "[*]";

  /** A UID that is used in older collections to represent the root of the document. */
  String ROOT_DOC_MARKER = "DOCROOT-a9fb1f04-0394-4c74-b77b-49b4e0ef7900";

  /** A UID that is used to represent an empty object, <code>{}</code>. */
  String EMPTY_OBJECT_MARKER = "EMPTYOBJ-bccbeee1-6173-4120-8492-7d7bafaefb1f";

  /** A UID that is used to represent an empty array, <code>[]</code>. */
  String EMPTY_ARRAY_MARKER = "EMPTYARRAY-9df4802a-c135-42d6-8be3-d23d9520a4e7";
}
