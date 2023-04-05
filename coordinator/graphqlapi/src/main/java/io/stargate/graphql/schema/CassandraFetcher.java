package io.stargate.graphql.schema;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.db.Parameters;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.graphql.web.StargateGraphqlContext;
import java.util.List;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

/** Base class for fetchers that access the Cassandra backend. It also handles authentication. */
public abstract class CassandraFetcher<ResultT> implements DataFetcher<ResultT> {

  public static final ConsistencyLevel DEFAULT_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM;
  public static final ConsistencyLevel DEFAULT_SERIAL_CONSISTENCY = ConsistencyLevel.SERIAL;
  public static final int DEFAULT_PAGE_SIZE = 100;

  public static final Parameters DEFAULT_PARAMETERS =
      Parameters.builder()
          .pageSize(DEFAULT_PAGE_SIZE)
          .consistencyLevel(DEFAULT_CONSISTENCY)
          .serialConsistencyLevel(DEFAULT_SERIAL_CONSISTENCY)
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

  protected boolean isAppliedBatch(List<Row> batchRows) {
    if (batchRows.isEmpty()) {
      return true;
    }
    if (batchRows.size() > 1) {
      return false;
    }
    Row row = batchRows.get(0);
    return row.columns().stream().map(Column::name).anyMatch("[applied]"::equals)
        && row.getBoolean("[applied]");
  }
}
