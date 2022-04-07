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
package io.stargate.sgv2.graphql.schema;

import com.google.protobuf.Int32Value;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.sgv2.common.grpc.proto.Rows;
import io.stargate.sgv2.graphql.web.resources.StargateGraphqlContext;
import java.util.List;

/** Base class for fetchers that access the Cassandra backend. */
public abstract class CassandraFetcher<ResultT> implements DataFetcher<ResultT> {

  public static final ConsistencyValue DEFAULT_CONSISTENCY =
      ConsistencyValue.newBuilder().setValue(Consistency.LOCAL_QUORUM).build();
  public static final ConsistencyValue DEFAULT_SERIAL_CONSISTENCY =
      ConsistencyValue.newBuilder().setValue(Consistency.SERIAL).build();
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final QueryParameters DEFAULT_PARAMETERS =
      QueryParameters.newBuilder()
          .setPageSize(Int32Value.of(DEFAULT_PAGE_SIZE))
          .setConsistency(DEFAULT_CONSISTENCY)
          .setSerialConsistency(DEFAULT_SERIAL_CONSISTENCY)
          .build();

  @Override
  public final ResultT get(DataFetchingEnvironment environment) throws Exception {

    // Small convenience: subclasses could just call environment.getContext() directly, but they'd
    // have to cast every time
    StargateGraphqlContext context = environment.getContext();

    return get(environment, context);
  }

  protected abstract ResultT get(
      DataFetchingEnvironment environment, StargateGraphqlContext context) throws Exception;

  protected boolean isApplied(ResultSet resultSet) {
    if (resultSet.getRowsCount() == 0) {
      return true;
    }
    return isApplied(resultSet.getRows(0), resultSet.getColumnsList());
  }

  protected boolean isApplied(Row row, List<ColumnSpec> columns) {
    int i = Rows.firstIndexOf("[applied]", columns);
    if (i < 0) {
      return false;
    }
    ColumnSpec column = columns.get(i);
    if (column.getType().getBasic() != TypeSpec.Basic.BOOLEAN) {
      return false;
    }
    return row.getValues(i).getBoolean();
  }
}
