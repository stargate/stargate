/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.cassandra.audit.DseAuditableEventType;
import com.datastax.bdp.db.audit.AuditableEventType;
import io.reactivex.Single;
import java.util.Map;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class RebuildSearchIndexStatement extends SearchIndexStatement {
  private final Map<String, String> requestOptions;

  public RebuildSearchIndexStatement(QualifiedName table, Map<String, String> requestOptions) {
    super(table);
    this.requestOptions = requestOptions;
  }

  @Override
  public AuditableEventType getAuditEventType() {
    return DseAuditableEventType.SOLR_REBUILD_SEARCH_STATEMENT;
  }

  @Override
  public Single<? extends ResultMessage> execute(
      QueryState queryState, QueryOptions queryOptions, long l) throws RequestExecutionException {
    logger.debug("Executing REBUILD SEARCH INDEX statement on {}", getCoreName());

    try {
      ResultMessage resultMessage = executeRemote(queryState, queryOptions, l).blockingGet();

      ClientWarn.instance.warn(getDCSpecificMessage());
    } catch (Exception e) {
      throw new InvalidRequestException(
          String.format(
              "Failed to rebuild search index %s because: %s",
              qualifiedName, formatMessage(e.getMessage())));
    }

    return Single.just(new ResultMessage.Void());
  }
}
