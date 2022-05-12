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

package io.stargate.sgv2.docsapi.api.common.properties.document;

/** Properties of a document table. */
public interface DocumentTableProperties {

  /** @return The name of the column where a document key is stored. */
  String keyColumnName();

  /** @return The name of the column where a leaf name is stored. */
  String leafColumnName();

  /** @return The name of the column where a string value is stored. */
  String stringValueColumnName();

  /** @return The name of the column where a double value is stored. */
  String doubleValueColumnName();

  /** @return The name of the column where a boolean value is stored. */
  String booleanValueColumnName();

  /** @return The prefix of the column where JSON path part is saved. */
  String pathColumnPrefix();

  /**
   * @param index Index of the path column
   * @return Returns path column name for the specific index
   */
  default String pathColumnName(int index) {
    if (index < 0) {
      throw new IllegalArgumentException("Index must not be negative.");
    }

    return pathColumnPrefix() + index;
  }

  /** @return The name of the column for the writetime of a row */
  default String writetimeColumnName() {
    return "writetime(leaf)";
  }
}
