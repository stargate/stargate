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
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ReloadSearchIndexStatement extends SearchIndexStatement {
  private final Map<String, String> requestOptions;

  public ReloadSearchIndexStatement(QualifiedName table, Map<String, String> requestOptions) {
    super(table);
    this.requestOptions = requestOptions;
  }

  @Override
  public AuditableEventType getAuditEventType() {
    return DseAuditableEventType.SOLR_RELOAD_SEARCH_INDEX;
  }

  @Override
  public Single<? extends ResultMessage> execute(
      QueryState queryState, QueryOptions queryOptions, long l) throws RequestExecutionException {
    logger.debug("Executing RELOAD SEARCH INDEX statement on {}", getCoreName());

    try {
      String oldSchema = getSchemaString(true);
      String newSchema = getSchemaString(false);

      ResultMessage resultMessage = executeRemote(queryState, queryOptions, l).blockingGet();

      if (resultMessage instanceof ResultMessage.SchemaChange) {
        ClientWarn.instance.warn(getDCSpecificMessage());

        if (oldSchema != null && newSchema != null) {
          String schemaChangedWarnings =
              SolrSchemaChangedWarnings.getWarningsRelatedToSchemaChange(oldSchema, newSchema);
          if (!schemaChangedWarnings.isEmpty()) {
            ClientWarn.instance.warn(schemaChangedWarnings);
          }
        }
      }
    } catch (Exception e) {
      throw new InvalidRequestException(
          String.format(
              "Failed to reload search index %s because: %s",
              qualifiedName, formatMessage(e.getMessage())));
    }

    ResultMessage.SchemaChange result =
        new ResultMessage.SchemaChange(
            new Event.SchemaChange(
                Event.SchemaChange.Change.UPDATED,
                Event.SchemaChange.Target.TABLE,
                keyspace(),
                name()));
    return Single.just(result);
  }
}
