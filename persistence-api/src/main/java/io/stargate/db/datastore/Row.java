/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.datastore;

import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.GettableByName;
import io.stargate.db.schema.Column;
import java.util.List;
import java.util.Objects;

public interface Row extends GettableByIndex, GettableByName {

  List<Column> columns();

  default boolean columnExists(String columnName) {
    List<Column> columns = columns();
    if (null == columns) {
      return false;
    }

    return columns.stream().anyMatch(c -> Objects.equals(c.name(), columnName));
  }

  @Override
  String toString();
}
