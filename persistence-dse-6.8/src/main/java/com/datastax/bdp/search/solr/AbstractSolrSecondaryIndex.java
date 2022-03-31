/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr;

import com.datastax.bdp.server.DseDaemon;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSolrSecondaryIndex implements Index {
  protected final Logger logger = LoggerFactory.getLogger(this.getClass());
  protected final ColumnFamilyStore baseCfs;
  protected final IndexMetadata indexDef;
  private final String indexName;

  public AbstractSolrSecondaryIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
    this.baseCfs = baseCfs;
    this.indexDef = indexDef;
    // this.coreInfo = new SolrCoreInfo(baseCfs.keyspace.getName(), baseCfs.getTableName());
    this.indexName = baseCfs.getTableName();
  }

  public static Map<String, String> validateOptions(Map<String, String> options) {
    Map<String, String> invalidOptions = new HashMap<>(options);
    invalidOptions.remove(IndexTarget.TARGET_OPTION_NAME);
    invalidOptions.remove(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
    return invalidOptions;
  }

  @Override
  public IndexBuildingSupport getBuildTaskSupport() {
    return null;
  }

  @Override
  public Indexer indexerFor(
      final DecoratedKey key,
      final RegularAndStaticColumns columns,
      final int nowInSec,
      final OpOrder.Group opGroup,
      final IndexTransaction.Type transactionType,
      Memtable memtable) {
    return null;
    /*
    switch (transactionType)
    {
        case CLEANUP:
            return new SolrCleanupIndexer(key);
        case UPDATE:
            return new SolrUpdateIndexer(key);
        default:
            return NOOP_INDEXER;
    }*/
  }

  @Override
  public Callable<?> getInitializationTask() {
    return null;
  }

  @Override
  public Callable<?> getPreJoinTask(boolean hadBootstrap) {
    return null;
  }

  @Override
  public Callable<?> getBlockingFlushTask() {
    return null;
  }

  @Override
  public IndexMetadata getIndexMetadata() {
    return indexDef;
  }

  @Override
  public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
    return () -> null;
  }

  @Override
  public void register(IndexRegistry registry) {
    registry.registerIndex(this);
  }

  @Override
  public Optional<ColumnFamilyStore> getBackingTable() {
    return Optional.empty();
  }

  @Override
  public Callable<?> getInvalidateTask() {
    return null;
  }

  @Override
  public Callable<?> getTruncateTask(long truncatedAt) {
    return null;
  }

  @Override
  public boolean dependsOn(ColumnMetadata column) {
    // TODO-SEARCH
    // A non-search node always assumes that a Solr secondary index depends on the
    // column, because it has no knowledge of the current schema without reading the
    // resources table. This means it is only possible to remove the column from a
    // search node (assuming it is no longer indexed in the Solr schema).
    return true;
  }

  @Override
  public boolean supportsExpression(ColumnMetadata column, Operator operator) {
    // This should never be called for Native CQL search queries (See
    // Cql3SolrSecondaryIndex#supportsOperator())
    return column.name.bytes.equals(DseDaemon.SOLR_QUERY_KEY_BB) && operator.equals(Operator.EQ);
  }

  @Override
  public AbstractType<?> customExpressionValueType() {
    return null;
  }

  @Override
  public long getEstimatedResultRows() {
    return 0;
  }

  @Override
  public void validate(PartitionUpdate update) throws InvalidRequestException {}

  @Override
  public boolean shouldBuildBlocking() {
    return true;
  }

  @Override
  public RowFilter postIndexQueryFilter(RowFilter filter) {
    throw new IllegalStateException(
        "["
            + getSolrCoreName()
            + "]: Solr queries should not be executed via Cassandra 2i searchers.");
  }

  @Override
  public Searcher searcherFor(ReadCommand command) {
    throw new IllegalStateException(
        "["
            + getSolrCoreName()
            + "]: Solr queries should not be executed via Cassandra 2i searchers.");
  }

  public String getSolrCoreName() {
    return indexName;
  }
}
