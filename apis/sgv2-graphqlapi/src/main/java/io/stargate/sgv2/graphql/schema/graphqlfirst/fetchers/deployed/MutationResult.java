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
package io.stargate.sgv2.graphql.schema.graphqlfirst.fetchers.deployed;

import io.stargate.bridge.proto.QueryOuterClass.ColumnSpec;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.bridge.proto.QueryOuterClass.ResultSet;
import io.stargate.bridge.proto.QueryOuterClass.Row;
import io.stargate.sgv2.api.common.grpc.proto.Rows;
import java.util.List;
import java.util.Optional;

interface MutationResult {

  static MutationResult forSingleQuery(Response response) {
    ResultSet rs = response.getResultSet();
    if (rs.getRowsCount() == 0) {
      return Applied.INSTANCE;
    } else {
      assert rs.getRowsCount() == 1; // always for single mutations
      Row row = rs.getRows(0);
      return rs.getColumnsList().stream().map(ColumnSpec::getName).anyMatch("[applied]"::equals)
              && Rows.getBoolean(row, "[applied]", rs.getColumnsList())
          ? Applied.INSTANCE
          : new NotApplied(Optional.of(row), rs.getColumnsList());
    }
  }

  class Applied implements MutationResult {
    static final Applied INSTANCE = new Applied();

    private Applied() {}
  }

  class NotApplied implements MutationResult {

    private final Optional<Row> row;
    private final List<ColumnSpec> columns;

    NotApplied(Optional<Row> row, List<ColumnSpec> columns) {
      this.row = row;
      this.columns = columns;
    }

    Optional<Row> getRow() {
      return row;
    }

    List<ColumnSpec> getColumns() {
      return columns;
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
