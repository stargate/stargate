/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.statements;

import static com.datastax.bdp.search.solr.core.SolrCoreResourceManager.BACKUP_SUFFIX;
import static java.util.stream.StreamSupport.stream;

import com.datastax.bdp.cassandra.auth.RowLevelAccessControlAuthorizer;
import com.datastax.bdp.cassandra.cql3.RlacWhitelistStatement;
import com.datastax.bdp.router.InternalQueryRouter;
import com.datastax.bdp.search.solr.core.SolrCoreResourceManager;
import com.datastax.bdp.search.solr.core.StargateCoreContainer;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.Addresses;
import com.google.common.collect.Sets;
import io.reactivex.Single;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.QualifiedStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract base class for Search Index management statements */
public abstract class SearchIndexStatement extends QualifiedStatement
    implements CQLStatement, RlacWhitelistStatement {
  private static final String DC_SPECIFIC_MSG =
      "Operation executed on all nodes in DC ${localDC}. ";
  private static final String MULTIPLE_DC_WARNING =
      "Please ensure that the operation is performed also on other search-enabled DCs: ${datacenters}";

  protected static final Logger logger = LoggerFactory.getLogger(SearchIndexStatement.class);

  private static final Set<String> VALID_BOOLEANS = Sets.newHashSet("true", "false");

  protected final Map<String, String> requestOptions;

  public SearchIndexStatement(QualifiedName table) {
    this(table, null);
  }

  public SearchIndexStatement(QualifiedName table, Map<String, String> requestOptions) {
    super(table);
    this.requestOptions = requestOptions;
  }

  protected String getCoreName() {
    return qualifiedName.toString();
  }

  public QualifiedName getQualifiedName() {
    return qualifiedName;
  }

  @Override
  public void validate(QueryState state) throws InvalidRequestException {
    if (null != RowLevelAccessControlAuthorizer.findRlacTargetColumn(keyspace(), name())) {
      throw new InvalidRequestException(
          "DSE Search may not be used on a table which has Row Level Access Control enabled.");
    }
  }

  @Nullable
  @Override
  public StagedScheduler getScheduler(QueryState queryState) {
    // Every Search index management operation either performs blocking reads and writes or
    // distributes the operation to other nodes and blocks on their responses.
    return TPC.ioScheduler();
  }

  @Override
  public CQLStatement prepare(ClientState state) {
    return this;
  }

  @Override
  public void authorize(QueryState state) throws UnauthorizedException {
    //            state.checkPermission(SearchResource.index(keyspace(), name()),
    //     getAccessPermission());
  }

  @Override
  public Single<? extends ResultMessage> executeLocally(QueryState state, QueryOptions options)
      throws RequestExecutionException {
    return execute(state, options, System.nanoTime());
  }

  protected boolean getBooleanOption(
      Map<String, String> options, String optionName, String optionDefault)
      throws InvalidRequestException {
    if ((options == null) || !options.containsKey(optionName)) {
      return Boolean.valueOf(optionDefault);
    }
    String optionValue = options.get(optionName);
    if (!VALID_BOOLEANS.contains(optionValue.toLowerCase())) {
      throw new InvalidRequestException(
          String.format("%s is not a valid boolean value for option %s", optionValue, optionName));
    }
    return Boolean.valueOf(optionValue);
  }

  protected static Set<String> getAllRemoteSearchEnabledDatacenters() {
    String myDC =
        EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress());

    return stream(Gossiper.instance.getAllEndpoints().spliterator(), false)
        .filter(endpoint -> !EndpointStateTracker.instance.getDatacenter(endpoint).equals(myDC))
        .filter(
            endpoint ->
                EndpointStateTracker.instance.getWorkloads(endpoint).contains(Workload.Search))
        .map(endpoint -> EndpointStateTracker.instance.getDatacenter(endpoint))
        .collect(Collectors.toSet());
  }

  public static String getDCSpecificMessage() {
    String localDC =
        EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress());
    Set<String> datacenters = getAllRemoteSearchEnabledDatacenters();

    StringBuilder message = new StringBuilder();
    message.append(DC_SPECIFIC_MSG);
    if (datacenters.size() > 0) {
      message.append(MULTIPLE_DC_WARNING);
    }
    return message
        .toString()
        .replace("${localDC}", localDC)
        .replace("${datacenters}", datacenters.toString());
  }

  protected Set<InetAddress> getRemoteSearchNodesInThisDatacenter() {
    String myDC =
        EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress());

    return stream(Gossiper.instance.getAllEndpoints().spliterator(), false)
        .filter(endpoint -> EndpointStateTracker.instance.getDatacenter(endpoint).equals(myDC))
        .filter(
            endpoint ->
                EndpointStateTracker.instance.getWorkloads(endpoint).contains(Workload.Search))
        .filter(endpoint -> !endpoint.equals(Addresses.Internode.getBroadcastAddress()))
        .collect(Collectors.toSet());
  }

  public Single<? extends ResultMessage> executeRemote(
      QueryState queryState, QueryOptions queryOptions, long l)
      throws RequestExecutionException, ExecutionException, InterruptedException {
    Set<InetAddress> remoteSearchEnabledNodesInThisDC = getRemoteSearchNodesInThisDatacenter();

    InternalQueryRouter internalQueryRouter = StargateCoreContainer.getInstance().getQueryRouter();

    CompletableFuture<ResultMessage> result =
        (CompletableFuture<ResultMessage>)
            internalQueryRouter.executeQueryRemote(
                remoteSearchEnabledNodesInThisDC.stream().findAny().get(),
                getQueryString(),
                queryState,
                queryOptions,
                new HashMap<>());

    return Single.just(TPCUtils.blockingGet(result));
  }

  public String getSchemaString(boolean backup) {
    String schemaXmlName = "schema.xml";
    schemaXmlName = (backup ? schemaXmlName + BACKUP_SUFFIX : schemaXmlName);

    try {
      return ByteBufferUtil.string(
          SolrCoreResourceManager.getInstance().tryReadResource(getCoreName(), schemaXmlName));
    } catch (Exception e) {
      logger.error(
          "Failed to read the existing schema for " + getCoreName() + " during schema update", e);
      throw new InvalidRequestException(
          "The search index schema could not be updated because: " + e.getMessage());
    }
  }

  // Using InternalQueryRouter, the exception recvd on the client (SG)
  // is of the type FailedProcessorException, removing this prefix
  // to mimic the exact error returned by the DSE Search node
  public String formatMessage(String exceptionMessage) {
    String pattern = "FailedProcessorException: ";
    String formattedMessage = exceptionMessage;
    int index = exceptionMessage.indexOf(pattern);

    if (index != -1) {
      formattedMessage = exceptionMessage.substring(index + pattern.length());
    }

    return formattedMessage;
  }
}
