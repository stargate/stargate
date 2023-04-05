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

import io.stargate.db.AbstractRowDecorator;
import io.stargate.db.ComparableKey;
import io.stargate.db.schema.TableName;
import java.util.stream.Collectors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.schema.TableMetadata;

public class RowDecoratorImpl extends AbstractRowDecorator {

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
}
