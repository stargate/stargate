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
package io.stargate.db;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public class DefaultQueryOptions<D> implements QueryOptions {

  private final ConsistencyLevel consistency;
  private final List<ByteBuffer> values;
  private final List<String> names;
  private final boolean skipMetadata;
  private final ProtocolVersion version;
  private final SpecificOptions options;

  public DefaultQueryOptions(
      ConsistencyLevel consistency,
      List<ByteBuffer> values,
      List<String> names,
      boolean skipMetadata,
      SpecificOptions options,
      ProtocolVersion version) {
    this.consistency = consistency;
    this.values = values;
    this.names = names;
    this.skipMetadata = skipMetadata;
    this.options = options;
    this.version = version;
  }

  private DefaultQueryOptions(DefaultQueryOptions.Builder<D> builder) {
    this.consistency = builder.consistency;
    this.values = builder.values;
    this.names = builder.names;
    this.skipMetadata = builder.skipMetadata;
    this.options = builder.options;
    this.version = builder.version;
  }

  @Override
  public ConsistencyLevel getConsistency() {
    return consistency;
  }

  @Override
  public List<ByteBuffer> getValues() {
    return values;
  }

  @Override
  public List<String> getNames() {
    return names;
  }

  @Override
  public ProtocolVersion getProtocolVersion() {
    return version;
  }

  @Override
  public int getPageSize() {
    return getSpecificOptions().pageSize;
  }

  @Override
  public ByteBuffer getPagingState() {
    return getSpecificOptions().pagingState;
  }

  @Override
  public ConsistencyLevel getSerialConsistency() {
    return getSpecificOptions().serialConsistency;
  }

  @Override
  public long getTimestamp() {
    return getSpecificOptions().timestamp;
  }

  @Override
  public int getNowInSeconds() {
    return getSpecificOptions().nowInSeconds;
  }

  /** The keyspace that this query is bound to, or null if not relevant. */
  @Override
  public String getKeyspace() {
    return getSpecificOptions().keyspace;
  }

  @Override
  public boolean skipMetadata() {
    return false;
  }

  private SpecificOptions getSpecificOptions() {
    return options;
  }

  public static DefaultQueryOptions.Builder<?> builder() {
    return new DefaultQueryOptions.Builder<>();
  }

  public static final class Builder<D> {
    private ConsistencyLevel consistency = ConsistencyLevel.ONE;
    private List<ByteBuffer> values = Collections.emptyList();
    private List<String> names = Collections.emptyList();
    private boolean skipMetadata;
    private ProtocolVersion version = ProtocolVersion.CURRENT;
    private SpecificOptions options;

    private Builder() {}

    public Builder<D> consistency(ConsistencyLevel consistency) {
      this.consistency = consistency;
      return this;
    }

    public Builder<D> values(List<ByteBuffer> values) {
      this.values = values;
      return this;
    }

    public Builder<D> names(List<String> names) {
      this.names = names;
      return this;
    }

    public Builder<D> skipMetadata(boolean skipMetadata) {
      this.skipMetadata = skipMetadata;
      return this;
    }

    public Builder<D> version(ProtocolVersion version) {
      this.version = version;
      return this;
    }

    public Builder<D> options(SpecificOptions options) {
      this.options = options;
      return this;
    }

    public DefaultQueryOptions<D> build() {
      return new DefaultQueryOptions<>(this);
    }
  }

  public static class SpecificOptions<T> {
    public static final SpecificOptions DEFAULT =
        new SpecificOptions(-1, null, null, Long.MIN_VALUE, null, Integer.MIN_VALUE);

    private int pageSize;
    private ByteBuffer pagingState;
    private ConsistencyLevel serialConsistency;
    private long timestamp;
    private String keyspace;
    private int nowInSeconds;

    public SpecificOptions(
        int pageSize,
        ByteBuffer state,
        ConsistencyLevel serialConsistency,
        long timestamp,
        String keyspace,
        int nowInSeconds) {
      this.pageSize = pageSize;
      this.pagingState = state;
      this.serialConsistency =
          serialConsistency == null ? ConsistencyLevel.SERIAL : serialConsistency;
      this.timestamp = timestamp;
      this.keyspace = keyspace;
      this.nowInSeconds = nowInSeconds;
    }

    private SpecificOptions(SpecificOptions.Builder<T> builder) {
      this.pageSize = builder.pageSize;
      this.pagingState = builder.pagingState;
      this.serialConsistency =
          builder.serialConsistency == null ? ConsistencyLevel.SERIAL : builder.serialConsistency;
      this.timestamp = builder.timestamp;
      this.keyspace = builder.keyspace;
      this.nowInSeconds = builder.nowInSeconds;
    }

    public static SpecificOptions.Builder<?> builder() {
      return new SpecificOptions.Builder<>();
    }

    public static final class Builder<T> {
      private int pageSize = -1;
      private ByteBuffer pagingState;
      private ConsistencyLevel serialConsistency;
      private long timestamp = Long.MIN_VALUE;
      private String keyspace;
      private int nowInSeconds = Integer.MIN_VALUE;

      private Builder() {}

      public Builder<T> pageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
      }

      public Builder<T> pagingState(ByteBuffer pagingState) {
        this.pagingState = pagingState;
        return this;
      }

      public Builder<T> serialConsistency(ConsistencyLevel serialConsistency) {
        this.serialConsistency = serialConsistency;
        return this;
      }

      public Builder<T> timestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
      }

      public Builder<T> keyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
      }

      public Builder<T> nowInSeconds(int nowInSeconds) {
        this.nowInSeconds = nowInSeconds;
        return this;
      }

      public SpecificOptions<T> build() {
        return new SpecificOptions<>(this);
      }
    }
  }
}
