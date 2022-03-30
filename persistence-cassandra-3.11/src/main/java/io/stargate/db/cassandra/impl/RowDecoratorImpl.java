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
package io.stargate.db.cassandra.impl;

import io.stargate.db.AbstractRowDecorator;
import io.stargate.db.ComparableKey;
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;

public class RowDecoratorImpl extends AbstractRowDecorator {

  private final CFMetaData metadata;

  public RowDecoratorImpl(TableName tableName) {
    this(tableName, Conversion.toCFMetaData(tableName));
  }

  private RowDecoratorImpl(TableName tableName, CFMetaData tableMetadata) {
    super(
        tableName,
        tableMetadata.partitionKeyColumns().stream()
            .map(c -> c.name.toCQLString())
            .collect(Collectors.toList()));
    this.metadata = tableMetadata;
  }

  @Override
  protected ComparableKey<?> decoratePrimaryKey(Object... pkValues) {
    Clustering key = metadata.getKeyValidatorAsClusteringComparator().make(pkValues);
    ByteBuffer serializedKey = CFMetaData.serializePartitionKey(key);
    DecoratedKey decoratedKey = metadata.partitioner.decorateKey(serializedKey);
    return new ComparableKey<>(PartitionPosition.class, decoratedKey);
  }

  @Override
  public Stream<Byte> getComparableBytes() {
    // TODO replace this with the relevant row's byte-comparable value when
    // https://github.com/apache/cassandra/pull/1294 is ready
    return Stream.empty();
  }
}
