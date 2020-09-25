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

import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.utils.MD5Digest;

public abstract class Result {
  public static Void VOID = new Void();

  public enum Kind {
    Void(1),
    Rows(2),
    SetKeyspace(3),
    Prepared(4),
    SchemaChange(5);

    public final int id;

    Kind(int id) {
      this.id = id;
    }

    public static Kind fromId(int id) {
      for (Kind k : values()) {
        if (k.id == id) return k;
      }
      throw new ProtocolException(String.format("Unknown kind id %d for RESULT message", id));
    }
  }

  public final Kind kind;
  private UUID tracingId;

  private Result(Kind kind) {
    this.kind = kind;
  }

  public Result setTracingId(UUID tracingId) {
    this.tracingId = tracingId;
    return this;
  }

  public UUID getTracingId() {
    return tracingId;
  }

  public static class Rows extends Result {
    public final List<List<ByteBuffer>> rows;
    public final ResultMetadata resultMetadata;

    public Rows(List<List<ByteBuffer>> rows, ResultMetadata resultMetadata) {
      super(Kind.Rows);
      this.rows = rows;
      this.resultMetadata = resultMetadata;
    }

    @Override
    public String toString() {
      return "ROWS " + resultMetadata;
    }
  }

  public static class Void extends Result {
    public Void() {
      super(Kind.Void);
    }

    @Override
    public String toString() {
      return "EMPTY RESULT";
    }
  }

  public static class SetKeyspace extends Result {
    public final String keyspace;

    public SetKeyspace(String keyspace) {
      super(Kind.SetKeyspace);
      this.keyspace = keyspace;
    }

    @Override
    public String toString() {
      return "RESULT set keyspace " + keyspace;
    }
  }

  public static class Prepared extends Result {
    public final MD5Digest statementId;
    public final MD5Digest resultMetadataId;
    public final ResultMetadata resultMetadata;
    public final PreparedMetadata metadata;

    public Prepared(
        MD5Digest statementId,
        MD5Digest resultMetadataId,
        ResultMetadata resultMetadata,
        PreparedMetadata preparedMetadata) {
      super(Kind.Prepared);
      this.statementId = statementId;
      this.resultMetadataId = resultMetadataId;
      this.resultMetadata = resultMetadata;
      this.metadata = preparedMetadata;
    }

    @Override
    public String toString() {
      return "RESULT PREPARED "
          + statementId
          + " "
          + metadata
          + " (resultMetadata="
          + resultMetadata
          + ")";
    }
  }

  public static class SchemaChange extends Result {
    public final SchemaChangeMetadata metadata;

    public SchemaChange(SchemaChangeMetadata metadata) {
      super(Kind.SchemaChange);
      this.metadata = metadata;
    }

    @Override
    public String toString() {
      return "RESULT schema change " + metadata;
    }
  }

  public enum Flag {
    // The order of that enum matters!!
    GLOBAL_TABLES_SPEC(1),
    HAS_MORE_PAGES(2),
    NO_METADATA(3),
    METADATA_CHANGED(4);

    public final int id;

    Flag(int id) {
      this.id = id;
    }

    public static Flag fromId(int id) {
      for (Flag f : values()) {
        if (f.id == id) return f;
      }
      throw new ProtocolException(String.format("Unknown flag id %d in RESULT message", id));
    }

    public static EnumSet<Flag> deserialize(int flags) {
      EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
      Flag[] values = Flag.values();
      for (int n = 0; n < values.length; n++) {
        if ((flags & (1 << n)) != 0) {
          set.add(values[n]);
        }
      }
      return set;
    }

    public static int serialize(EnumSet<Flag> flags) {
      int i = 0;
      for (Flag flag : flags) {
        i |= 1 << flag.ordinal();
      }
      return i;
    }
  }

  public static class ResultMetadata {
    public static final ResultMetadata EMPTY =
        new ResultMetadata(
            EnumSet.of(Flag.NO_METADATA),
            Collections.emptyList(),
            MD5Digest.compute(new byte[0]),
            null);

    public final EnumSet<Flag> flags;
    public final int columnCount;
    public final List<Column> columns;
    public final MD5Digest resultMetadataId;
    public ByteBuffer pagingState;

    public ResultMetadata(
        EnumSet<Flag> flags,
        int columnCount,
        List<Column> columns,
        MD5Digest resultMetadataId,
        ByteBuffer pagingState) {
      this.flags = flags;
      this.columnCount = columnCount;
      this.columns = columns;
      this.resultMetadataId = resultMetadataId;
      this.pagingState = pagingState;
    }

    public ResultMetadata(
        EnumSet<Flag> flags,
        List<Column> columns,
        MD5Digest resultMetadataId,
        ByteBuffer pagingState) {
      this.flags = flags;
      this.columnCount = columns.size();
      this.columns = columns;
      this.resultMetadataId = resultMetadataId;
      this.pagingState = pagingState;
    }
  }

  public static class PreparedMetadata {
    public final EnumSet<Flag> flags;
    public final List<Column> columns;
    public final short[] partitionKeyBindIndexes;

    public PreparedMetadata(
        EnumSet<Flag> flags, List<Column> columns, short[] partitionKeyBindIndexes) {
      this.flags = flags;
      this.columns = columns;
      this.partitionKeyBindIndexes = partitionKeyBindIndexes;
    }
  }

  public static class SchemaChangeMetadata {
    public final String change;
    public final String target;
    public final String keyspace;
    public final String name;
    public final List<String> argTypes;

    public SchemaChangeMetadata(
        String change, String target, String keyspace, String name, List<String> argTypes) {
      this.change = change;
      this.target = target;
      this.keyspace = keyspace;
      this.name = name;
      if (!target.equals("KEYSPACE")) {
        assert this.name != null
            : "Table, type, function or aggregate name should be set for non-keyspace schema change events";
      }
      this.argTypes = argTypes;
    }
  }
}
