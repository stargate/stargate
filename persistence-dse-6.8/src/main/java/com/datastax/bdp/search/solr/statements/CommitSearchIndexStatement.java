/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.cassandra.audit.DseAuditableEventType;
import com.datastax.bdp.db.audit.AuditableEventType;
import io.reactivex.Single;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CommitSearchIndexStatement extends SearchIndexStatement {
  public CommitSearchIndexStatement(QualifiedName table) {
    super(table);
  }

  @Override
  public AuditableEventType getAuditEventType() {
    return DseAuditableEventType.SOLR_COMMIT_SEARCH_INDEX_STATEMENT;
  }

  @Override
  public Single<? extends ResultMessage> execute(
      QueryState queryState, QueryOptions queryOptions, long l) throws RequestExecutionException {
    logger.debug("Executing COMMIT SEARCH INDEX statement on {}", getCoreName());

    try {
      ResultMessage resultMessage = executeRemote(queryState, queryOptions, l).blockingGet();
    } catch (Exception e) {
      throw new InvalidRequestException(
          String.format(
              "Failed to commit search index %s because: %s",
              qualifiedName, formatMessage(e.getMessage())));
    }

    return Single.just(new ResultMessage.Void());
  }
}
