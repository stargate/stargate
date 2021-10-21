package com.datastax.bdp.search.solr;

import java.util.Optional;
import java.util.concurrent.Callable;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
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
import org.jetbrains.annotations.Nullable;

/**
 * Empty implementation for DSE search indexes.
 *
 * <p>This is used as a placeholder when our DSE backend contains search indexes. Search queries
 * won't work through Stargate, but it gets us past the schema parsing phase, and we are able to
 * handle other queries.
 */
public class Cql3SolrSecondaryIndex implements Index {

  private static final Callable<?> EMPTY_TASK = () -> null;

  private final IndexMetadata indexMetadata;

  public Cql3SolrSecondaryIndex(ColumnFamilyStore ignoredCfs, IndexMetadata indexMetadata) {
    this.indexMetadata = indexMetadata;
  }

  @Override
  public Callable<?> getInitializationTask() {
    return EMPTY_TASK;
  }

  @Override
  public IndexMetadata getIndexMetadata() {
    return indexMetadata;
  }

  @Override
  public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
    return EMPTY_TASK;
  }

  @Override
  public void register(IndexRegistry indexRegistry) {
    // intentionally empty
  }

  @Override
  public Optional<ColumnFamilyStore> getBackingTable() {
    return Optional.empty();
  }

  @Nullable
  @Override
  public Callable<?> getBlockingFlushTask() {
    return EMPTY_TASK;
  }

  @Override
  public Callable<?> getInvalidateTask() {
    return EMPTY_TASK;
  }

  @Override
  public Callable<?> getTruncateTask(long l) {
    return EMPTY_TASK;
  }

  @Override
  public boolean shouldBuildBlocking() {
    return false;
  }

  @Override
  public boolean dependsOn(ColumnMetadata columnMetadata) {
    return false;
  }

  @Override
  public boolean supportsExpression(ColumnMetadata columnMetadata, Operator operator) {
    return false;
  }

  @Override
  public AbstractType<?> customExpressionValueType() {
    // indicates custom expressions are not supported
    return null;
  }

  @Override
  public long getEstimatedResultRows() {
    return 0;
  }

  @Override
  public void validate(PartitionUpdate partitionUpdate) throws InvalidRequestException {
    // intentionally empty
  }

  @Override
  public Indexer indexerFor(
      DecoratedKey decoratedKey,
      RegularAndStaticColumns regularAndStaticColumns,
      int i,
      OpOrder.Group group,
      IndexTransaction.Type type,
      Memtable memtable) {
    // indicates we're not interested in the update
    return null;
  }

  @Override
  public Searcher searcherFor(ReadCommand readCommand) {
    // Per original implementation
    throw new IllegalStateException(
        "Solr queries should not be executed via Cassandra 2i searchers.");
  }

  @Override
  public RowFilter postIndexQueryFilter(RowFilter rowFilter) {
    // Per original implementation
    throw new IllegalStateException(
        "Solr queries should not be executed via Cassandra 2i searchers.");
  }
}
