package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.search.solr.core.CassandraCoreRequestOptions;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.util.Addresses;
import com.google.common.base.Throwables;
import io.reactivex.Single;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.solr.handler.admin.CoreAdminConstants;

public class CreateSearchIndexStatement extends SearchIndexStatement {
  public static final String INDEX_CREATED_MSG = "Search index successfully created ";
  public static final String DISTRIBUTED_WARNING =
      "in distributed mode; executed ${operation} on all nodes in DC ${localDC}.";
  public static final String NON_DISTRIBUTED_WARNING =
      "in non-distributed mode; executed ${operation} only on the node ${node}";

  public static final String MULTIPLE_NODES_WARNING =
      "Please ensure that ${operation} is performed on other nodes in DC ${localDC}.";
  public static final String CREATE_WITH_REINDEX = "index reload and reindex";
  public static final String CREATE_WITHOUT_REINDEX = "index reload";

  public CreateSearchIndexStatement(
      QualifiedName table,
      boolean ifNotExists,
      List<String> columns,
      Map<String, Map<String, String>> columnOptions,
      List<String> profiles,
      Map<String, String> configOptions,
      Map<String, String> requestOptions) {
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
      QueryState queryState, QueryOptions queryOptions, long l) {
    logger.debug("Executing CREATE SEARCH INDEX statement on {}", getCoreName());

    try {
      CassandraCoreRequestOptions coreReqOptions = buildRequestOptions();
      Set<String> remoteSearchEnabledDCs = getAllRemoteSearchEnabledDatacenters();
      Set<InetAddress> remoteSearchEnabledNodesInThisDC = getRemoteSearchNodesInThisDatacenter();

      ResultMessage resultMessage = executeRemote(queryState, queryOptions, l).blockingGet();

      HashMap<String, String> messageValues = new HashMap<>();
      messageValues.put(
          "operation", coreReqOptions.isReindex() ? CREATE_WITH_REINDEX : CREATE_WITHOUT_REINDEX);
      messageValues.put("node", Addresses.Internode.getBroadcastAddress().toString());
      messageValues.put("datacenters", getAllRemoteSearchEnabledDatacenters().toString());
      messageValues.put(
          "localDC",
          EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress()));

      StrSubstitutor sub = new StrSubstitutor(messageValues);

      StringBuilder messageBuilder = new StringBuilder();
      messageBuilder.append(INDEX_CREATED_MSG);
      messageBuilder.append(
          coreReqOptions.isDistributed() ? DISTRIBUTED_WARNING : NON_DISTRIBUTED_WARNING);
      if (!coreReqOptions.isDistributed() && remoteSearchEnabledNodesInThisDC.size() > 0) {
        messageBuilder.append('\n');
        messageBuilder.append(MULTIPLE_NODES_WARNING);
      }
      if (remoteSearchEnabledDCs.size() > 0) {
        messageBuilder.append('\n');
        messageBuilder.append(getDCSpecificMessage());
      }

      ClientWarn.instance.warn(sub.replace(messageBuilder.toString()));
    } catch (Exception e) {
      String message = e.getMessage();
      if (e.getCause() != null) {
        message = message + ": " + Throwables.getRootCause(e).getMessage();
      }
      throw new InvalidRequestException(
          String.format(
              "Failed to create search index %s because: %s",
              qualifiedName, formatMessage(message)));
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

  private CassandraCoreRequestOptions buildRequestOptions() {
    CassandraCoreRequestOptions.Builder builder = new CassandraCoreRequestOptions.Builder();
    builder.setReindex(
        getBooleanOption(
            requestOptions, CoreAdminConstants.REINDEX, CoreAdminConstants.CREATE_REINDEX_DEFAULT));
    builder.setDistributed(
        getBooleanOption(
            requestOptions,
            CoreAdminConstants.DISTRIBUTED,
            CoreAdminConstants.DISTRIBUTED_DEFAULT));
    boolean recovery =
        getBooleanOption(
            requestOptions, CoreAdminConstants.RECOVERY, CoreAdminConstants.RECOVERY_DEFAULT);
    builder.setRecovery(recovery);
    builder.setDeleteAll(recovery);

    CassandraCoreRequestOptions options = builder.build();
    // validate the options and add any warnings
    if (!options.isReindex()) {
      ClientWarn.instance.warn(
          "You requested the core to be created with "
              + CoreAdminConstants.REINDEX
              + "=false. Preexisting data will not be searchable via DSE Search until you reindex.");
    }

    return options;
  }
}
