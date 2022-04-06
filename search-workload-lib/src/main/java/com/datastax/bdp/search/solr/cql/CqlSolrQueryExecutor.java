/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import static java.util.stream.StreamSupport.stream;
import static org.apache.cassandra.db.ConsistencyLevel.LOCAL_ONE;
import static org.apache.cassandra.db.ConsistencyLevel.ONE;

import com.datastax.bdp.router.InternalQueryRouter;
import com.datastax.bdp.search.solr.core.StargateCoreContainer;
import com.datastax.bdp.server.CoreSystemInfo;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.Addresses;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execute a CQL statement as a Solr query if such statement contains a single, and only,
 * "solr_query" WHERE clause. <br>
 * CQL statements with Solr queries support:
 *
 * <ul>
 *   <li>Columns selection (but no distinct).
 *   <li>Limit clause (defaults to 100).
 * </ul>
 *
 * The Solr query execution model is an hybrid scatter/gather between Solr and Cassandra:
 *
 * <ul>
 *   <li>The "solr_query" value is extracted and turned into an actual Solr query request via a
 *       {@link QueryParser}. rows.
 * </ul>
 */
public class CqlSolrQueryExecutor {
  /**
   * If true, forces ShardHandler creation and usage, overriding default behavior in SearchHandler.
   * Mandatory to be compatible in mixed version clusters pre 5.1+DSP-14898. See DSP-14993. Can be
   * removed when 5.1 is dead.
   */
  public static final String FORCE_SHARD_HANDLER = "ForceShardHandler";

  public static final int DEFAULT_ROWS = 10;
  public static final String SOLR_QUERY_HANDLER = "solr_query";
  public static final String SOLR_QUERY_PAGING_DRIVER_VALUE = "driver";
  public static final String SOLR_QUERY_PAGING_OFF_VALUE = "off";
  public static final Set<String> SOLR_QUERY_PAGING_VALUES =
      ImmutableSet.of(SOLR_QUERY_PAGING_DRIVER_VALUE, SOLR_QUERY_PAGING_OFF_VALUE);
  public static final Set<ConsistencyLevel> SUPPORTED_CONSISTENCY_LEVELS =
      ImmutableSet.of(ONE, LOCAL_ONE);

  public static final String NON_UNIQUE_KEY_MULTI_ROW_DOC_ERROR =
      "Search queries are not allowed when the document unique key does not match the CQL primary key unless "
          + "the query is a count or the selected columns are a subset of the unique key.";

  private static final int DRIVER_PAGING_DISABLED_VALUE = -1;

  private static final Logger logger = LoggerFactory.getLogger(CqlSolrQueryExecutor.class);
  private static final NoSpamLogger noSpamLogger =
      NoSpamLogger.getLogger(logger, 180, TimeUnit.SECONDS);

  private static final Joiner commaJoiner = Joiner.on(',');

  private final boolean isAnalyticsWorkload =
      Workload.Analytics.isCompatibleWith(CoreSystemInfo.getWorkloads());

  public ResultMessage.Rows execute(
      String query,
      SolrSelectStatement statement,
      QueryOptions queryOptions,
      QueryState queryState,
      Map<String, ByteBuffer> payload)
      throws InvalidRequestException, SolrRequestExecutionException, TimeoutException {
    try {
      InternalQueryRouter internalQueryRouter =
          StargateCoreContainer.getInstance().getQueryRouter();
      InetAddress nodeBroadcastAddress =
          getRemoteSearchNodesInThisDatacenter().iterator().next(); // TODO-SEARCH
      Future<ResultMessage> future =
          internalQueryRouter.executeQueryRemote(
              nodeBroadcastAddress, query, queryState, queryOptions, payload);
      // TODO-SEARCH change time out settings
      return (ResultMessage.Rows) TPCUtils.blockingGet(future, 5, TimeUnit.SECONDS);
      // TODO-SEARCH
      // return doExecute(statement, queryOptions, state);
    } catch (Throwable ex) {
      // SolrCoreInfo info = new SolrCoreInfo(statement.getKeyspace(), statement.getColumnFamily());
      String indexName = statement.getKeyspace() + "." + statement.getColumnFamily();
      StringBuilder errorBuilder = new StringBuilder();
      errorBuilder.append("Error during solr query for index ").append(indexName);

      // handle not logging SolrException twice
      if (exceptionNotLoggedBySolr(ex)) {
        // log index information plus full exception info
        logger.error(errorBuilder.toString(), ex);
      } else {
        // only log index information
        logger.error(errorBuilder.toString());
      }

      // since the query may contain some confidential data we only log it if it has been
      // explicitly allowed by the customer using
      // nodetool setlogginglevel com.datastax.bdp.search.solr.cql.CqlSolrQueryExecutor DEBUG
      // or the like
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Query that failed filters on: " + statement.describeFilter(queryState, queryOptions));
      }
      throw ex;
    }
  }

  private boolean exceptionNotLoggedBySolr(Throwable t) {
    return ((t.getCause() == null)
        || !(t.getCause() instanceof SolrException)
        || !((SolrException) t.getCause()).logException());
  }

  private Set<InetAddress> getRemoteSearchNodesInThisDatacenter() {
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
}
