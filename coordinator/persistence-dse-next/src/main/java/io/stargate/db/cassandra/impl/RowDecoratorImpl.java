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
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowDecoratorImpl extends AbstractRowDecorator {

  private static final Logger LOG = LoggerFactory.getLogger(RowDecoratorImpl.class);

  private final TableMetadata metadata;

  public RowDecoratorImpl(TableName tableName) {
    this(tableName, Conversion.toTableMetadata(tableName));
  }

  private RowDecoratorImpl(TableName tableName, TableMetadata tableMetadata) {
    super(
        tableName,
        tableMetadata.partitionKeyColumns().stream()
            .map(c -> c.name.toCQLString())
            .collect(Collectors.toList()));
    this.metadata = tableMetadata;
  }

  private DecoratedKey decoratedKeyFromRawKeyValues(Object... rawKeyValues) {
    Clustering<?> key = metadata.partitionKeyAsClusteringComparator().make(rawKeyValues);
    return metadata.partitioner.decorateKey(key.serializeAsPartitionKey());
  }

  @Override
  protected ComparableKey<?> decoratePrimaryKey(Object... rawKeyValues) {
    DecoratedKey decoratedKey = decoratedKeyFromRawKeyValues(rawKeyValues);
    return new ComparableKey<>(PartitionPosition.class, decoratedKey);
  }

  /**
   * {@inheritDoc}
   *
   * <p><b>IMPORTANT:</b> we only support getting the bytes for the Murmur3Partitioner,
   * ByteOrderedPartitioner & OrderPreservingPartitioner. Any other partitioner will only depend on
   * the key comparison, which is not going to work for the correct OR queries in the Docs API V2.
   */
  @Override
  public ByteBuffer getComparableBytes(Row row) {
    DecoratedKey decoratedKey = decoratedKeyFromRawKeyValues(primaryKeyValues(row));
    ByteComparable.Version version = ByteComparable.Version.OSS41;
    ByteSource keyComparableBytes = ByteSource.of(decoratedKey.getKey(), version);
    ByteSource tokenComparableBytes = getTokenComparableBytes(decoratedKey.getToken(), version);

    // combine
    ByteSource bytes =
        ByteSource.withTerminator(ByteSource.TERMINATOR, tokenComparableBytes, keyComparableBytes);
    return ByteBuffer.wrap(readBytes(bytes, 64));
  }

  // resolves the comparable bytes for three token types
  private static ByteSource getTokenComparableBytes(Token token, ByteComparable.Version version) {
    // Murmur3Partitioner is default, so first try that
    // https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/dht/Murmur3Partitioner.java#L182-L186
    if (token instanceof Murmur3Partitioner.LongToken) {
      long tokenValue = (long) token.getTokenValue();
      return ByteSource.of(tokenValue);
    }

    // https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/dht/ByteOrderedPartitioner.java#L107-L111
    if (token instanceof ByteOrderedPartitioner.BytesToken) {
      byte[] tokenValue = (byte[]) token.getTokenValue();
      return ByteSource.of(tokenValue, version);
    }

    // https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/dht/OrderPreservingPartitioner.java#L206-L210
    if (token instanceof OrderPreservingPartitioner.StringToken) {
      String tokenValue = (String) token.getTokenValue();
      return ByteSource.of(tokenValue, version);
    }

    // we only support the above 3
    // for other partitioner, log warn and wrap empty bytes
    LOG.warn(
        "Comparable bytes not supported for the token type {}.", token.getClass().getSimpleName());
    return ByteSource.EMPTY;
  }

  // copied from com.datastax.bdp.db.tries.util.ByteSourceUtil
  public static byte[] readBytes(ByteSource byteSource, final int initialBufferCapacity) {
    if (byteSource == null) return new byte[0];

    int readBytes = 0;
    byte[] buf = new byte[initialBufferCapacity];
    int data;
    while ((data = byteSource.next()) != ByteSource.END_OF_STREAM) {
      // inlined com.datastax.bdp.db.tries.util.ByteSourceUtil#ensureCapacity
      if (readBytes == buf.length) {
        buf = Arrays.copyOf(buf, readBytes * 2);
      }
      buf[readBytes++] = (byte) (data & 0xFF);
    }

    if (readBytes != buf.length) {
      buf = Arrays.copyOf(buf, readBytes);
    }
    return buf;
  }
}
