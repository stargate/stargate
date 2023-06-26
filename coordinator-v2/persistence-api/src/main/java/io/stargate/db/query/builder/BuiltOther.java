package io.stargate.db.query.builder;

import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.Query;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.TypedValue;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BuiltOther extends BuiltQuery<BuiltOther.Bound> {
  private final String queryString;

  BuiltOther(TypedValue.Codec codec, AsyncQueryExecutor executor, String queryString) {
    this(codec, null, executor, queryString);
  }

  private BuiltOther(
      TypedValue.Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      String queryString) {
    super(QueryType.OTHER, codec, preparedId, executor, Collections.emptyList());
    this.queryString = queryString;
  }

  @Override
  public QueryType type() {
    return QueryType.OTHER;
  }

  @Override
  public String queryStringForPreparation() {
    return queryString;
  }

  @Override
  protected BuiltOther.Bound createBoundQuery(List<TypedValue> values) {
    return new Bound(this);
  }

  @Override
  public Query<Bound> withPreparedId(MD5Digest preparedId) {
    return new BuiltOther(valueCodec(), preparedId, executor(), queryString);
  }

  @Override
  public String toString() {
    return queryString;
  }

  public static class Bound extends AbstractBound<BuiltOther> {
    private Bound(BuiltOther builtQuery) {
      super(builtQuery, Collections.emptyList(), Collections.emptyList());
    }
  }
}
