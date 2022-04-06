/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.cassandra.audit.DseAuditableEventType;
import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.xml.XmlPath;
import io.reactivex.Single;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;

public class AlterSearchIndexStatement extends SearchIndexStatement {
  private final boolean config;
  private final String verb;
  private final XmlPath path;
  private final String attribute;
  private final String value;
  private final String json;

  public AlterSearchIndexStatement(
      QualifiedName table,
      boolean config,
      String verb,
      XmlPath path,
      String attribute,
      String value,
      String json) {
    super(table);
    this.config = config;
    this.verb = verb;
    this.path = path;
    this.attribute = attribute;
    this.value = value;
    this.json = json;
  }

  @Override
  public AuditableEventType getAuditEventType() {
    return DseAuditableEventType.SOLR_ALTER_SEARCH_INDEX_STATEMENT;
  }

  @Override
  public Single<? extends ResultMessage> execute(
      QueryState queryState, QueryOptions queryOptions, long l) throws RequestExecutionException {
    logger.debug(
        "Executing ALTER SEARCH INDEX statement on {} with config={}, verb={}, path={}, attribute={}, value={}, json={}",
        getCoreName(),
        config,
        verb,
        path,
        attribute,
        value,
        json);

    try {
      String currentSchema = getSchemaString(false);

      ResultMessage resultMessage = executeRemote(queryState, queryOptions, l).blockingGet();

      if (resultMessage instanceof ResultMessage.SchemaChange) {
        // get the schema post alter statement completes
        String newSchema = getSchemaString(false);

        String schemaChangedWarnings =
            SolrSchemaChangedWarnings.getWarningsRelatedToSchemaChange(currentSchema, newSchema);

        if (!schemaChangedWarnings.isEmpty()) {
          ClientWarn.instance.warn(schemaChangedWarnings);
        }
      }
    } catch (Exception e) {
      throw new InvalidRequestException(
          String.format(
              "Failed to alter search index %s because: %s",
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
