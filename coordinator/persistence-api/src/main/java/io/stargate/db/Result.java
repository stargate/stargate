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

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.utils.MD5Digest;

public abstract class Result {
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
  private @Nullable List<String> warnings;

  private Result(Kind kind) {
    this.kind = kind;
  }

  public Result setTracingId(UUID tracingId) {
    this.tracingId = tracingId;
    return this;
  }

  public Result setWarnings(@Nullable List<String> warnings) {
    Preconditions.checkState(this.warnings == null, "Warnings have already been set.");
    this.warnings = warnings;
    return this;
  }

  public UUID getTracingId() {
    return tracingId;
  }

  public @Nullable List<String> getWarnings() {
    return warnings;
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

  @SuppressWarnings("JavaLangClash")
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
    public final boolean isIdempotent;
    public final boolean isUseKeyspace;

    public Prepared(
        MD5Digest statementId,
        MD5Digest resultMetadataId,
        ResultMetadata resultMetadata,
        PreparedMetadata preparedMetadata,
        boolean isIdempotent,
        boolean isUseKeyspace) {
      super(Kind.Prepared);
      this.statementId = statementId;
      this.resultMetadataId = resultMetadataId;
      this.resultMetadata = resultMetadata;
      this.metadata = preparedMetadata;
      this.isIdempotent = isIdempotent;
      this.isUseKeyspace = isUseKeyspace;
    }

    @Override
    public String toString() {
      return "RESULT PREPARED "
          + statementId
          + " "
          + metadata
          + " (resultMetadata="
          + resultMetadata
          + ")"
          + " idIdempotent="
          + isIdempotent;
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
    /**
     * The definition of the columns for each row in the result set. Please note that the objects in
     * this list are not "true" columns, in the sense that 1) some may not correspond to a declared
     * column at all (for instance, {@code ttl(c)}) and 2) even for definition that correspond to a
     * genuine column, some of information may not be set. For instance, the {@link Column#kind()}
     * method may well return {@code null}, even if the column is otherwise a partition column. The
     * only things that can be safely relied upon for a column are it's keyspace and table name, the
     * name of what it represents and the type.
     *
     * <p>Practically, if you get a column {@code c} from this list that correspond to a genuine
     * column "c" in table "k.t", and {@code t} is that {@link io.stargate.db.schema.Table} schema,
     * do no assume that {@code c.equals(t.column(c.name()))} (because the {@link Column#kind} or
     * {@link Column#order} may differ).
     */
    // TODO: we may want to try to have a specific class here instead of Column to make it very
    //  clear that those aren't entirely the same concept that the Column object in the schema.
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
      // The column count indicates the number of columns expected in a result
      // (see https://github.com/stargate/stargate/pull/2760 for details).
      // We can (and should) safely drop the remaining columns.
      this.columns =
          columns.size() == columnCount
              ? columns
              : columns.stream().limit(columnCount).collect(Collectors.toList());
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
      assert target.equals("KEYSPACE") || this.name != null
          : "Table, type, function or aggregate name should be set for non-keyspace schema change events";
      this.argTypes = argTypes;
    }
  }
}
