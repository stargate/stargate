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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.graphql.schema.graphqlfirst.fetchers.deployed;

import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import java.util.List;
import java.util.Optional;

interface MutationResult {

  static MutationResult forSingleQuery(ResultSet rs) {
    List<Row> rows = rs.currentPageRows();
    if (rows.isEmpty()) {
      return Applied.INSTANCE;
    } else {
      assert rows.size() == 1; // always for single mutations
      Row row = rows.get(0);
      return row.columns().stream().map(Column::name).anyMatch("[applied]"::equals)
              && row.getBoolean("[applied]")
          ? Applied.INSTANCE
          : new NotApplied(row);
    }
  }

  class Applied implements MutationResult {
    static final Applied INSTANCE = new Applied();

    private Applied() {}
  }

  class NotApplied implements MutationResult {
    static final NotApplied NO_ROW = new NotApplied(Optional.empty());

    private final Optional<Row> row;

    NotApplied(Row row) {
      this(Optional.of(row));
    }

    private NotApplied(Optional<Row> row) {
      this.row = row;
    }

    Optional<Row> getRow() {
      return row;
    }
  }

  class Failure implements MutationResult {
    private final Throwable error;

    Failure(Throwable error) {
      this.error = error;
    }

    Throwable getError() {
      return error;
    }
  }
}
