package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import io.reactivex.Single;
import java.util.Map;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

public class DropSearchIndexStatement extends SearchIndexStatement {
  public DropSearchIndexStatement(QualifiedName table, Map<String, String> requestOptions) {
    super(table, requestOptions);
  }

  @Override
  public AuditableEventType getAuditEventType() {
    return null;
  }

  @Override
  public void validate(QueryState state) throws InvalidRequestException {
    super.validate(state);
  }

  @Override
  public Single<? extends ResultMessage> execute(
      QueryState queryState, QueryOptions queryOptions, long l) throws RequestExecutionException {
    logger.debug("Executing DROP SEARCH INDEX statement on {}", getCoreName());

    try {
      ResultMessage resultMessage = executeRemote(queryState, queryOptions, l).blockingGet();
    } catch (Exception e) {
      throw new InvalidRequestException(
          String.format(
              "Failed to drop search index %s because: %s",
              qualifiedName, formatMessage(e.getMessage())));
    }

    return Single.just(
        new ResultMessage.SchemaChange(
            new Event.SchemaChange(
                Event.SchemaChange.Change.UPDATED,
                Event.SchemaChange.Target.TABLE,
                keyspace(),
                name())));
  }
}
