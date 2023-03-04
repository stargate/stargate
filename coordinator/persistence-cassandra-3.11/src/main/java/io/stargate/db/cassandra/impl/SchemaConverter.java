package io.stargate.db.cassandra.impl;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import io.stargate.db.datastore.common.AbstractCassandraSchemaConverter;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Kind;
import io.stargate.db.schema.Column.Order;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;

public class SchemaConverter
    extends AbstractCassandraSchemaConverter<
        KeyspaceMetadata, CFMetaData, ColumnDefinition, UserType, IndexMetadata, ViewDefinition> {

  @Override
  protected Set<String> getExcludedIndexOptions() {
    return ImmutableSet.of(IndexTarget.CUSTOM_INDEX_OPTION_NAME, IndexTarget.TARGET_OPTION_NAME);
  }

  @Override
  protected String keyspaceName(KeyspaceMetadata keyspace) {
    return keyspace.name;
  }

  @Override
  protected Map<String, String> replicationOptions(KeyspaceMetadata keyspace) {
    return keyspace.params.replication.asMap();
  }

  @Override
  protected boolean usesDurableWrites(KeyspaceMetadata keyspace) {
    return keyspace.params.durableWrites;
  }

  @Override
  protected Iterable<CFMetaData> tables(KeyspaceMetadata keyspace) {
    return keyspace.tables;
  }

  @Override
  protected Iterable<UserType> userTypes(KeyspaceMetadata keyspace) {
    return keyspace.types;
  }

  @Override
  protected Iterable<ViewDefinition> views(KeyspaceMetadata keyspace) {
    return keyspace.views;
  }

  @Override
  protected String tableName(CFMetaData table) {
    return table.cfName;
  }

  @Override
  protected Iterable<ColumnDefinition> columns(CFMetaData table) {
    return table::allColumnsInSelectOrder;
  }

  @Override
  protected String columnName(ColumnDefinition column) {
    return column.name.toString();
  }

  @Override
  protected ColumnType columnType(ColumnDefinition column) {
    return Conversion.getTypeFromInternal(column.type);
  }

  @Override
  protected Order columnClusteringOrder(ColumnDefinition column) {
    switch (column.clusteringOrder()) {
      case ASC:
        return Column.Order.ASC;
      case DESC:
        return Column.Order.DESC;
      case NONE:
        return null;
      default:
        throw new IllegalStateException("Clustering columns should always have an order");
    }
  }

  @Override
  protected Kind columnKind(ColumnDefinition column) {
    switch (column.kind) {
      case REGULAR:
        return Column.Kind.Regular;
      case STATIC:
        return Column.Kind.Static;
      case PARTITION_KEY:
        return Column.Kind.PartitionKey;
      case CLUSTERING:
        return Column.Kind.Clustering;
    }
    throw new IllegalStateException("Unknown column kind");
  }

  @Override
  protected Iterable<IndexMetadata> secondaryIndexes(CFMetaData table) {
    // This is an unfortunate departure from the rest of this class being 'stateless'. This
    // is really a weakness of the C* schema code that some of the index information is not
    // part of schema objects and that we should reach into the ColumnFamilyStore, but there is
    // nothing we can do at the Stargate level...
    return Iterables.transform(
        Keyspace.openAndGetStore(table).indexManager.listIndexes(), Index::getIndexMetadata);
  }

  @Override
  protected String comment(CFMetaData table) {
    return table.params.comment;
  }

  @Override
  protected int ttl(CFMetaData table) {
    return table.params.defaultTimeToLive;
  }

  @Override
  protected String indexName(IndexMetadata index) {
    return index.name;
  }

  @Override
  protected String indexTarget(IndexMetadata index) {
    return index.options.get("target");
  }

  @Override
  protected boolean isCustom(IndexMetadata index) {
    return index.kind == IndexMetadata.Kind.CUSTOM;
  }

  @Override
  protected String indexClass(IndexMetadata index) {
    return index.options.get("class_name");
  }

  @Override
  protected Map<String, String> indexOptions(IndexMetadata index) {
    return index.options.entrySet().stream()
        .filter(x -> !getExcludedIndexOptions().contains(x.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  protected List<Column> userTypeFields(UserType userType) {
    return Conversion.getUDTColumns(userType);
  }

  @Override
  protected String userTypeName(UserType userType) {
    return userType.getNameAsString();
  }

  @Override
  protected CFMetaData asTable(ViewDefinition view) {
    return view.metadata;
  }

  @Override
  protected boolean isBaseTableOf(CFMetaData table, ViewDefinition view) {
    return view.baseTableId.equals(table.cfId);
  }
}
