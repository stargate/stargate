/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.stargate.transport.internal.messages;

import io.netty.buffer.ByteBuf;
import io.stargate.db.Result;
import io.stargate.db.schema.Column;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.internal.CBCodec;
import org.apache.cassandra.stargate.transport.internal.CBUtil;
import org.apache.cassandra.stargate.transport.internal.ColumnUtils;
import org.apache.cassandra.stargate.transport.internal.Message;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class ResultMessage extends Message.Response {
  public static final Message.Codec<ResultMessage> codec =
      new Message.Codec<ResultMessage>() {
        @Override
        public ResultMessage decode(ByteBuf body, ProtocolVersion version) {
          Result.Kind kind = Result.Kind.fromId(body.readInt());
          return new ResultMessage(SUBCODECS.get(kind).decode(body, version));
        }

        @Override
        public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version) {
          dest.writeInt(msg.result.kind.id);
          SUBCODECS.get(msg.result.kind).encode(msg.result, dest, version);
        }

        @Override
        public int encodedSize(ResultMessage msg, ProtocolVersion version) {
          return 4 + SUBCODECS.get(msg.result.kind).encodedSize(msg.result, version);
        }
      };

  public static Map<Result.Kind, CBCodec<Result>> SUBCODECS = new EnumMap<>(Result.Kind.class);

  static {
    SUBCODECS.put(Result.Kind.Void, new VoidSubCodec());
    SUBCODECS.put(Result.Kind.SetKeyspace, new SetKeyspaceSubCodec());
    SUBCODECS.put(Result.Kind.Rows, new RowsSubCodec());
    SUBCODECS.put(Result.Kind.Prepared, new PreparedSubCodec());
    SUBCODECS.put(Result.Kind.SchemaChange, new SchemaChangeSubCodec());
  }

  public final Result result;

  protected ResultMessage(Result result) {
    super(Message.Type.RESULT);
    this.result = result;
    this.tracingId = result.getTracingId();
    this.warnings = result.getWarnings();
  }

  public static class VoidSubCodec implements CBCodec<Result> {
    @Override
    public Result decode(ByteBuf body, ProtocolVersion version) {
      return new Result.Void();
    }

    @Override
    public void encode(Result result, ByteBuf dest, ProtocolVersion version) {
      assert result instanceof Result.Void;
    }

    @Override
    public int encodedSize(Result result, ProtocolVersion version) {
      return 0;
    }
  }

  public static class SetKeyspaceSubCodec implements CBCodec<Result> {
    @Override
    public Result decode(ByteBuf body, ProtocolVersion version) {
      String keyspace = CBUtil.readString(body);
      return new Result.SetKeyspace(keyspace);
    }

    @Override
    public void encode(Result result, ByteBuf dest, ProtocolVersion version) {
      assert result instanceof Result.SetKeyspace;
      CBUtil.writeAsciiString(((Result.SetKeyspace) result).keyspace, dest);
    }

    @Override
    public int encodedSize(Result result, ProtocolVersion version) {
      assert result instanceof Result.SetKeyspace;
      return CBUtil.sizeOfAsciiString(((Result.SetKeyspace) result).keyspace);
    }
  }

  public static class RowsSubCodec implements CBCodec<Result> {
    public static final CBCodec<Result.ResultMetadata> METADATA_CODEC =
        new CBCodec<Result.ResultMetadata>() {
          @Override
          public Result.ResultMetadata decode(ByteBuf body, ProtocolVersion version) {
            // flags & column count
            int iflags = body.readInt();
            int columnCount = body.readInt();

            EnumSet<Result.Flag> flags = Result.Flag.deserialize(iflags);

            MD5Digest resultMetadataId = null;
            if (flags.contains(Result.Flag.METADATA_CHANGED)) {
              assert version.isGreaterOrEqualTo(ProtocolVersion.V5)
                  : "MetadataChanged flag is not supported before native protocol v5";
              assert !flags.contains(Result.Flag.NO_METADATA)
                  : "MetadataChanged and NoMetadata are mutually exclusive flags";

              resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
            }

            ByteBuffer pagingState = null;
            if (flags.contains(Result.Flag.HAS_MORE_PAGES)) pagingState = CBUtil.readValue(body);

            if (flags.contains(Result.Flag.NO_METADATA))
              return new Result.ResultMetadata(flags, columnCount, null, null, pagingState);

            boolean globalTablesSpec = flags.contains(Result.Flag.GLOBAL_TABLES_SPEC);

            String globalKsName = null;
            String globalCfName = null;
            if (globalTablesSpec) {
              globalKsName = CBUtil.readString(body);
              globalCfName = CBUtil.readString(body);
            }

            // metadata (names/types)
            List<Column> columns = new ArrayList<Column>(columnCount);
            for (int i = 0; i < columnCount; i++) {
              String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
              String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
              columns.add(ColumnUtils.decodeColumn(body, version, ksName, cfName));
            }

            return new Result.ResultMetadata(flags, columns, resultMetadataId, pagingState);
          }

          @Override
          public void encode(
              Result.ResultMetadata metadata, ByteBuf dest, ProtocolVersion version) {
            boolean noMetadata = metadata.flags.contains(Result.Flag.NO_METADATA);
            boolean globalTablesSpec = metadata.flags.contains(Result.Flag.GLOBAL_TABLES_SPEC);
            boolean hasMorePages = metadata.flags.contains(Result.Flag.HAS_MORE_PAGES);
            boolean metadataChanged = metadata.flags.contains(Result.Flag.METADATA_CHANGED);
            assert version.isGreaterThan(ProtocolVersion.V1) || (!hasMorePages && !noMetadata)
                : "version = " + version + ", flags = " + metadata.flags;

            dest.writeInt(Result.Flag.serialize(metadata.flags));
            dest.writeInt(metadata.columnCount);

            if (hasMorePages) CBUtil.writeValue(metadata.pagingState, dest);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5) && metadataChanged) {
              assert !noMetadata : "MetadataChanged and NoMetadata are mutually exclusive flags";
              CBUtil.writeBytes(metadata.resultMetadataId.bytes, dest);
            }

            if (!noMetadata) {
              if (globalTablesSpec) {
                CBUtil.writeAsciiString(metadata.columns.get(0).keyspace(), dest);
                CBUtil.writeAsciiString(metadata.columns.get(0).table(), dest);
              }

              for (Column c : metadata.columns) {
                if (!globalTablesSpec) {
                  CBUtil.writeAsciiString(c.keyspace(), dest);
                  CBUtil.writeAsciiString(c.table(), dest);
                }
                ColumnUtils.encodeColumn(c, dest, version);
              }
            }
          }

          @Override
          public int encodedSize(Result.ResultMetadata metadata, ProtocolVersion version) {
            boolean noMetadata = metadata.flags.contains(Result.Flag.NO_METADATA);
            boolean globalTablesSpec = metadata.flags.contains(Result.Flag.GLOBAL_TABLES_SPEC);
            boolean hasMorePages = metadata.flags.contains(Result.Flag.HAS_MORE_PAGES);
            boolean metadataChanged = metadata.flags.contains(Result.Flag.METADATA_CHANGED);

            int size = 8;
            if (hasMorePages) size += CBUtil.sizeOfValue(metadata.pagingState);

            if (version.isGreaterOrEqualTo(ProtocolVersion.V5) && metadataChanged)
              size += CBUtil.sizeOfBytes(metadata.resultMetadataId.bytes);

            if (!noMetadata) {
              if (globalTablesSpec) {
                size += CBUtil.sizeOfAsciiString(metadata.columns.get(0).keyspace());
                size += CBUtil.sizeOfAsciiString(metadata.columns.get(0).table());
              }

              for (Column c : metadata.columns) {
                if (!globalTablesSpec) {
                  size += CBUtil.sizeOfAsciiString(c.keyspace());
                  size += CBUtil.sizeOfAsciiString(c.table());
                }
                size += ColumnUtils.encodeSizeColumn(c, version);
              }
            }
            return size;
          }
        };

    @Override
    public Result decode(ByteBuf body, ProtocolVersion version) {
      Result.ResultMetadata metadata = METADATA_CODEC.decode(body, version);
      int rowCount = body.readInt();
      List<List<ByteBuffer>> rows = new ArrayList<List<ByteBuffer>>(rowCount);

      for (int i = 0; i < rowCount; ++i) {
        List<ByteBuffer> values = new ArrayList<>(metadata.columnCount);
        for (int j = 0; j < metadata.columnCount; ++j) {
          values.add(CBUtil.readValue(body));
        }
        rows.add(values);
      }

      return new Result.Rows(rows, metadata);
    }

    @Override
    public void encode(Result result, ByteBuf dest, ProtocolVersion version) {
      assert result instanceof Result.Rows;
      Result.Rows rows = (Result.Rows) result;
      METADATA_CODEC.encode(rows.resultMetadata, dest, version);
      dest.writeInt(rows.rows.size());
      for (List<ByteBuffer> row : rows.rows) {
        for (int i = 0; i < rows.resultMetadata.columnCount; ++i)
          CBUtil.writeValue(row.get(i), dest);
      }
    }

    @Override
    public int encodedSize(Result result, ProtocolVersion version) {
      assert result instanceof Result.Rows;
      Result.Rows rows = (Result.Rows) result;
      int size = METADATA_CODEC.encodedSize(rows.resultMetadata, version);
      for (List<ByteBuffer> row : rows.rows) {
        for (int i = 0; i < rows.resultMetadata.columnCount; ++i)
          size += CBUtil.sizeOfValue(row.get(i));
      }
      return size;
    }
  }

  public static class PreparedSubCodec implements CBCodec<Result> {
    public static final CBCodec<Result.PreparedMetadata> METADATA_CODEC =
        new CBCodec<Result.PreparedMetadata>() {
          @Override
          public Result.PreparedMetadata decode(ByteBuf body, ProtocolVersion version) {
            // flags & column count
            int iflags = body.readInt();
            int columnCount = body.readInt();

            EnumSet<Result.Flag> flags = Result.Flag.deserialize(iflags);

            short[] partitionKeyBindIndexes = null;
            if (version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
              int numPKNames = body.readInt();
              if (numPKNames > 0) {
                partitionKeyBindIndexes = new short[numPKNames];
                for (int i = 0; i < numPKNames; i++) partitionKeyBindIndexes[i] = body.readShort();
              }
            }

            boolean globalTablesSpec = flags.contains(Result.Flag.GLOBAL_TABLES_SPEC);

            String globalKsName = null;
            String globalCfName = null;
            if (globalTablesSpec) {
              globalKsName = CBUtil.readString(body);
              globalCfName = CBUtil.readString(body);
            }

            // metadata (names/types)
            List<Column> columns = new ArrayList<Column>(columnCount);
            for (int i = 0; i < columnCount; i++) {
              String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
              String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
              columns.add(ColumnUtils.decodeColumn(body, version, ksName, cfName));
            }

            return new Result.PreparedMetadata(flags, columns, partitionKeyBindIndexes);
          }

          @Override
          public void encode(
              Result.PreparedMetadata metadata, ByteBuf dest, ProtocolVersion version) {

            boolean globalTablesSpec = metadata.flags.contains(Result.Flag.GLOBAL_TABLES_SPEC);
            dest.writeInt(Result.Flag.serialize(metadata.flags));
            dest.writeInt(metadata.columns.size());

            if (version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
              // there's no point in providing partition key bind indexes if the statements affect
              // multiple tables
              if (metadata.partitionKeyBindIndexes == null || !globalTablesSpec) {
                dest.writeInt(0);
              } else {
                dest.writeInt(metadata.partitionKeyBindIndexes.length);
                for (Short bindIndex : metadata.partitionKeyBindIndexes) dest.writeShort(bindIndex);
              }
            }

            if (globalTablesSpec) {
              CBUtil.writeAsciiString(metadata.columns.get(0).keyspace(), dest);
              CBUtil.writeAsciiString(metadata.columns.get(0).table(), dest);
            }

            for (Column c : metadata.columns) {
              if (!globalTablesSpec) {
                CBUtil.writeAsciiString(c.keyspace(), dest);
                CBUtil.writeAsciiString(c.table(), dest);
              }
              ColumnUtils.encodeColumn(c, dest, version);
            }
          }

          @Override
          public int encodedSize(Result.PreparedMetadata metadata, ProtocolVersion version) {
            boolean globalTablesSpec = metadata.flags.contains(Result.Flag.GLOBAL_TABLES_SPEC);
            int size = 8;
            if (globalTablesSpec) {
              size += CBUtil.sizeOfAsciiString(metadata.columns.get(0).keyspace());
              size += CBUtil.sizeOfAsciiString(metadata.columns.get(0).table());
            }

            if (metadata.partitionKeyBindIndexes != null
                && version.isGreaterOrEqualTo(ProtocolVersion.V4))
              size += 4 + 2 * metadata.partitionKeyBindIndexes.length;

            for (Column c : metadata.columns) {
              if (!globalTablesSpec) {
                size += CBUtil.sizeOfAsciiString(c.keyspace());
                size += CBUtil.sizeOfAsciiString(c.table());
              }
              size += ColumnUtils.encodeSizeColumn(c, version);
            }
            return size;
          }
        };

    @Override
    public Result decode(ByteBuf body, ProtocolVersion version) {
      MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
      MD5Digest resultMetadataId = null;
      if (version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2))
        resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
      Result.PreparedMetadata metadata = METADATA_CODEC.decode(body, version);

      Result.ResultMetadata resultMetadata = Result.ResultMetadata.EMPTY;
      if (version.isGreaterThan(ProtocolVersion.V1))
        resultMetadata = RowsSubCodec.METADATA_CODEC.decode(body, version);

      // it is safe to return false as isIdempotent, as this information
      // is not used in the native protocol
      // we need to hard-code it because the Result.Prepared is used as the external type,
      // but also internal propagated by specific persistence backends
      return new Result.Prepared(id, resultMetadataId, resultMetadata, metadata, false, false);
    }

    @Override
    public void encode(Result result, ByteBuf dest, ProtocolVersion version) {
      assert result instanceof Result.Prepared;
      Result.Prepared prepared = (Result.Prepared) result;
      assert prepared.statementId != null;

      CBUtil.writeBytes(prepared.statementId.bytes, dest);
      if (version.isGreaterOrEqualTo(ProtocolVersion.V5, ProtocolVersion.DSE_V2))
        CBUtil.writeBytes(prepared.resultMetadataId.bytes, dest);

      METADATA_CODEC.encode(prepared.metadata, dest, version);
      if (version.isGreaterThan(ProtocolVersion.V1))
        RowsSubCodec.METADATA_CODEC.encode(prepared.resultMetadata, dest, version);
    }

    @Override
    public int encodedSize(Result result, ProtocolVersion version) {
      assert result instanceof Result.Prepared;
      Result.Prepared prepared = (Result.Prepared) result;
      assert prepared.statementId != null;

      int size = 0;
      size += CBUtil.sizeOfBytes(prepared.statementId.bytes);
      if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
        size += CBUtil.sizeOfBytes(prepared.resultMetadataId.bytes);
      size += METADATA_CODEC.encodedSize(prepared.metadata, version);
      if (version.isGreaterThan(ProtocolVersion.V1))
        size += RowsSubCodec.METADATA_CODEC.encodedSize(prepared.resultMetadata, version);
      return size;
    }
  }

  public static class SchemaChangeSubCodec implements CBCodec<Result> {
    public static final CBCodec<Result.SchemaChangeMetadata> METADATA_CODEC =
        new CBCodec<Result.SchemaChangeMetadata>() {
          @Override
          public Result.SchemaChangeMetadata decode(ByteBuf body, ProtocolVersion version) {
            return METADATA_CODEC.decode(body, version);
          }

          @Override
          public void encode(
              Result.SchemaChangeMetadata metadata, ByteBuf dest, ProtocolVersion version) {
            METADATA_CODEC.encode(metadata, dest, version);
          }

          @Override
          public int encodedSize(Result.SchemaChangeMetadata metadata, ProtocolVersion version) {
            return METADATA_CODEC.encodedSize(metadata, version);
          }
        };

    @Override
    public Result decode(ByteBuf body, ProtocolVersion version) {
      throw new UnsupportedOperationException("TODO");
      // return new SchemaChange(Event.SchemaChange.deserializeEvent(body, version));
    }

    @Override
    public void encode(Result result, ByteBuf dest, ProtocolVersion version) {
      assert result instanceof Result.SchemaChange;
      Result.SchemaChange change = (Result.SchemaChange) result;
      Result.SchemaChangeMetadata metadata = change.metadata;
      if (metadata.target.equals("FUNCTION") || metadata.target.equals("AGGREGATE")) {
        if (version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
          // available since protocol version 4
          CBUtil.writeAsciiString(metadata.change, dest);
          CBUtil.writeAsciiString(metadata.target, dest);
          CBUtil.writeAsciiString(metadata.keyspace, dest);
          CBUtil.writeAsciiString(metadata.name, dest);
          CBUtil.writeStringList(metadata.argTypes, dest);
        } else {
          // not available in protocol versions < 4 - just say the keyspace was updated.
          CBUtil.writeAsciiString("UPDATED", dest);
          if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
            CBUtil.writeAsciiString("KEYSPACE", dest);
          CBUtil.writeAsciiString(metadata.keyspace, dest);
          CBUtil.writeAsciiString("", dest);
        }
        return;
      }

      if (version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
        CBUtil.writeAsciiString(metadata.change, dest);
        CBUtil.writeAsciiString(metadata.target, dest);
        CBUtil.writeAsciiString(metadata.keyspace, dest);
        if (!metadata.target.equals("KEYSPACE")) CBUtil.writeAsciiString(metadata.name, dest);
      } else {
        if (metadata.target.equals("TYPE")) {
          // For the v1/v2 protocol, we have no way to represent type changes, so we simply say the
          // keyspace
          // was updated.  See CASSANDRA-7617.
          CBUtil.writeAsciiString("UPDATED", dest);
          CBUtil.writeAsciiString(metadata.keyspace, dest);
          CBUtil.writeAsciiString("", dest);
        } else {
          CBUtil.writeAsciiString(metadata.change, dest);
          CBUtil.writeAsciiString(metadata.keyspace, dest);
          CBUtil.writeAsciiString(metadata.target.equals("KEYSPACE") ? "" : metadata.name, dest);
        }
      }
    }

    @Override
    public int encodedSize(Result result, ProtocolVersion version) {
      assert result instanceof Result.SchemaChange;
      Result.SchemaChange change = (Result.SchemaChange) result;
      Result.SchemaChangeMetadata metadata = change.metadata;
      if (metadata.target.equals("FUNCTION") || metadata.target.equals("AGGREGATE")) {
        if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
          return CBUtil.sizeOfAsciiString(metadata.change)
              + CBUtil.sizeOfAsciiString(metadata.target)
              + CBUtil.sizeOfAsciiString(metadata.keyspace)
              + CBUtil.sizeOfAsciiString(metadata.name)
              + CBUtil.sizeOfStringList(metadata.argTypes);
        if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
          return CBUtil.sizeOfAsciiString("UPDATED")
              + CBUtil.sizeOfAsciiString("KEYSPACE")
              + CBUtil.sizeOfAsciiString(metadata.keyspace);
        return CBUtil.sizeOfAsciiString("UPDATED")
            + CBUtil.sizeOfAsciiString(metadata.keyspace)
            + CBUtil.sizeOfAsciiString("");
      }

      if (version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
        int size =
            CBUtil.sizeOfAsciiString(metadata.change)
                + CBUtil.sizeOfAsciiString(metadata.target)
                + CBUtil.sizeOfAsciiString(metadata.keyspace);

        if (!metadata.target.equals("KEYSPACE")) size += CBUtil.sizeOfAsciiString(metadata.name);

        return size;
      } else {
        if (metadata.target.equals("TYPE")) {
          return CBUtil.sizeOfAsciiString("UPDATED")
              + CBUtil.sizeOfAsciiString(metadata.keyspace)
              + CBUtil.sizeOfAsciiString("");
        }
        return CBUtil.sizeOfAsciiString(metadata.change)
            + CBUtil.sizeOfAsciiString(metadata.keyspace)
            + CBUtil.sizeOfAsciiString(metadata.target.equals("KEYSPACE") ? "" : metadata.name);
      }
    }
  }
}
