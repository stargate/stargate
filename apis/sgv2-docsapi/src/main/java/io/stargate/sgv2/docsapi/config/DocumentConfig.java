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

package io.stargate.sgv2.docsapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;

/**
 * Configuration for the documents.
 *
 * <p><b>IMPORTANT:</b> Do not inject this class, but rather {@link
 * io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties}.
 */
@ConfigMapping(prefix = "stargate.document")
public interface DocumentConfig {

  /** @return Defines the maximum depth of the JSON document, defaults to <code>64</code>. */
  @Max(64)
  @Positive
  @WithDefault("64")
  int maxDepth();

  /** @return Defines the maximum array length in a JSON field, defaults to <code>1000000</code>. */
  @Max(1000000)
  @Positive
  @WithDefault("1000000")
  int maxArrayLength();

  /** @return Defines the maximum document page size, defaults to <code>20</code>. */
  @Max(50)
  @Positive
  @WithDefault("20")
  int maxPageSize();

  /**
   * @return Defines the maximum Cassandra search page size when fetching documents, defaults to
   *     <code>1000
   *     </code>.
   */
  @Max(10000)
  @Positive
  @WithDefault("1000")
  int maxSearchPageSize();

  /** {@inheritDoc} */
  DocumentTableConfig table();

  interface DocumentTableConfig {

    /** @return The name of the column where a document key is stored. */
    @NotBlank
    @WithDefault(Constants.KEY_COLUMN_NAME)
    String keyColumnName();

    /** @return The name of the column where a leaf name is stored. */
    @NotBlank
    @WithDefault(Constants.LEAF_COLUMN_NAME)
    String leafColumnName();

    /** @return The name of the column where a string value is stored. */
    @NotBlank
    @WithDefault(Constants.STRING_VALUE_COLUMN_NAME)
    String stringValueColumnName();

    /** @return The name of the column where a double value is stored. */
    @NotBlank
    @WithDefault(Constants.DOUBLE_VALUE_COLUMN_NAME)
    String doubleValueColumnName();

    /** @return The name of the column where a boolean value is stored. */
    @NotBlank
    @WithDefault(Constants.BOOLEAN_VALUE_COLUMN_NAME)
    String booleanValueColumnName();

    /** @return The prefix of the column where JSON path part is saved. */
    @NotBlank
    @WithDefault(Constants.PATH_COLUMN_PREFIX)
    String pathColumnPrefix();
  }
}
