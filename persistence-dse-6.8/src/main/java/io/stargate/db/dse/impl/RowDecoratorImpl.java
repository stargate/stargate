/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.dse.impl;

import static org.apache.cassandra.utils.ByteSource.END_OF_STREAM;

import io.stargate.db.AbstractRowDecorator;
import io.stargate.db.ComparableKey;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.TableName;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteComparable;
import org.apache.cassandra.utils.ByteSource;
import org.apache.cassandra.utils.ByteSource.Peekable;

public class RowDecoratorImpl extends AbstractRowDecorator {

  /**
   * Utility to treat a ByteSource as an Iterable, to easily convert to Stream. This can likely be
   * reused in the other RowDecoratorImpl's once ByteSource is available in Cassandra 3.11/4.0.
   */
  private static class ByteSourceIterator implements Iterator<Byte> {
    Peekable src;

    public ByteSourceIterator(ByteSource src) {
      this.src = new Peekable(src);
    }

    @Override
    public Byte next() {
      return (byte) src.next();
    }

    @Override
    public boolean hasNext() {
      return src.peek() != END_OF_STREAM;
    }
  }

  private final TableMetadata metadata;

  public RowDecoratorImpl(TableName tableName, TableMetadata tableMetadata) {
    super(
        tableName,
        tableMetadata.partitionKeyColumns().stream()
            .map(c -> c.name.toCQLString())
            .collect(Collectors.toList()));
    this.metadata = tableMetadata;
  }

  @Override
  protected ComparableKey<?> decoratePrimaryKey(Object... rawKeyValues) {
    Clustering key = metadata.partitionKeyAsClusteringComparator().make(rawKeyValues);
    DecoratedKey decoratedKey = metadata.partitioner.decorateKey(key.serializeAsPartitionKey());
    return new ComparableKey<>(PartitionPosition.class, decoratedKey);
  }

  @Override
  public Stream<Byte> getComparableBytes(Row row) {
    Clustering key = metadata.partitionKeyAsClusteringComparator().make(primaryKeyValues(row));
    DecoratedKey decoratedKey = metadata.partitioner.decorateKey(key.serializeAsPartitionKey());
    ByteSource src = decoratedKey.asComparableBytes(ByteComparable.Version.DSE68);
    Spliterator<Byte> spliterator =
        Spliterators.spliteratorUnknownSize(new ByteSourceIterator(src), 0);
    return StreamSupport.stream(spliterator, false);
  }
}
