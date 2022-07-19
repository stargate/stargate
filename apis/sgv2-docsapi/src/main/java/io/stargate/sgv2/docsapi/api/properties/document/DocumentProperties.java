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

package io.stargate.sgv2.docsapi.api.properties.document;

/** General document-related properties. */
public interface DocumentProperties {

  /** @return Defines the maximum depth of the JSON document. */
  int maxDepth();

  /** @return Defines the maximum array length in a JSON field. */
  int maxArrayLength();

  /** @return Defines the maximum document page size. */
  int maxPageSize();

  /** @return Defines the Cassandra search page size when fetching documents. */
  int maxSearchPageSize();

  /** @return Properties for a table where documents are stored. */
  DocumentTableProperties tableProperties();

  /** @return The helper for resolving column names in a document table. */
  DocumentTableColumns tableColumns();

  /**
   * @return Returns approximate storage page size to use when querying database, based on the
   *     amount of documents we are searching for. We consider that in average documents have 16
   *     fields.
   */
  default int getApproximateStoragePageSize(int numberOfDocuments) {
    return Math.min(numberOfDocuments * 16, maxSearchPageSize());
  }
}
