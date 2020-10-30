/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stargate.producer.kafka.helpers;

import static io.stargate.producer.kafka.schema.SchemasTestConstants.COLUMN_NAME;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.internal.core.type.codec.CqlDurationCodec;
import com.datastax.oss.driver.internal.core.type.codec.InetCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Table;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.stargate.db.Cell;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;

public class MutationEventHelper {

  @NonNull
  public static RowUpdateEvent createRowUpdateEvent(
      String partitionKeyValue,
      Column partitionKeyMetadata,
      String columnValue,
      Column columnMetadata,
      Integer clusteringKeyValue,
      Column clusteringKeyMetadata,
      Table tableMetadata) {
    return createRowUpdateEvent(
        partitionKeyValue,
        partitionKeyMetadata,
        columnValue,
        columnMetadata,
        clusteringKeyValue,
        clusteringKeyMetadata,
        tableMetadata,
        0);
  }

  @NonNull
  public static RowUpdateEvent createRowUpdateEventNoPk(
      String columnValue,
      Column columnMetadata,
      Integer clusteringKeyValue,
      Column clusteringKeyMetadata,
      Table tableMetadata) {
    return createRowUpdateEvent(
        Collections.emptyList(),
        Collections.singletonList(cell(columnMetadata, columnValue)),
        Collections.singletonList(cellValue(clusteringKeyValue, clusteringKeyMetadata)),
        tableMetadata,
        0);
  }

  @NonNull
  public static RowUpdateEvent createRowUpdateEventNoCK(
      String partitionKeyValue,
      Column partitionKeyMetadata,
      String columnValue,
      Column columnMetadata,
      Table tableMetadata) {
    return createRowUpdateEvent(
        Collections.singletonList(cellValue(partitionKeyValue, partitionKeyMetadata)),
        Collections.singletonList(cell(columnMetadata, columnValue)),
        Collections.emptyList(),
        tableMetadata,
        0);
  }

  @NonNull
  public static RowUpdateEvent createRowUpdateEventNoColumns(
      String partitionKeyValue,
      Column partitionKeyMetadata,
      Integer clusteringKeyValue,
      Column clusteringKeyMetadata,
      Table tableMetadata) {
    return createRowUpdateEvent(
        Collections.singletonList(cellValue(partitionKeyValue, partitionKeyMetadata)),
        Collections.emptyList(),
        Collections.singletonList(cellValue(clusteringKeyValue, clusteringKeyMetadata)),
        tableMetadata,
        0);
  }

  @NonNull
  public static RowUpdateEvent createRowUpdateEvent(
      String partitionKeyValue,
      Column partitionKeyMetadata,
      Object columnValue,
      Column columnMetadata,
      Integer clusteringKeyValue,
      Column clusteringKeyMetadata,
      Table tableMetadata,
      long timestamp) {
    return createRowUpdateEvent(
        Collections.singletonList(cellValue(partitionKeyValue, partitionKeyMetadata)),
        Collections.singletonList(cell(columnMetadata, columnValue)),
        Collections.singletonList(cellValue(clusteringKeyValue, clusteringKeyMetadata)),
        tableMetadata,
        timestamp);
  }

  @NonNull
  public static RowUpdateEvent createRowUpdateEvent(
      List<CellValue> partitionKeys,
      List<Cell> cells,
      List<CellValue> clusteringKeys,
      Table tableMetadata) {
    return createRowUpdateEvent(partitionKeys, cells, clusteringKeys, tableMetadata, 0);
  }

  @NonNull
  public static RowUpdateEvent createRowUpdateEvent(
      List<CellValue> partitionKeys,
      List<Cell> cells,
      List<CellValue> clusteringKeys,
      Table tableMetadata,
      long timestamp) {
    return new RowUpdateEvent() {
      @Override
      public Table getTable() {
        return tableMetadata;
      }

      @Override
      public long getTimestamp() {
        return timestamp;
      }

      @Override
      public List<CellValue> getPartitionKeys() {
        return partitionKeys;
      }

      @Override
      public List<CellValue> getClusteringKeys() {
        return clusteringKeys;
      }

      @Override
      public List<Cell> getCells() {
        return cells;
      }
    };
  }

  @NonNull
  public static DeleteEvent createDeleteEvent(
      String partitionKeyValue,
      Column partitionKeyMetadata,
      Integer clusteringKeyValue,
      Column clusteringKeyMetadata,
      Table tableMetadata) {
    return createDeleteEvent(
        partitionKeyValue,
        partitionKeyMetadata,
        clusteringKeyValue,
        clusteringKeyMetadata,
        tableMetadata,
        0);
  }

  @NonNull
  public static DeleteEvent createDeleteEvent(
      String partitionKeyValue,
      Column partitionKeyMetadata,
      Integer clusteringKeyValue,
      Column clusteringKeyMetadata,
      Table tableMetadata,
      long timestamp) {
    return createDeleteEvent(
        Collections.singletonList(cellValue(partitionKeyValue, partitionKeyMetadata)),
        Collections.singletonList(cellValue(clusteringKeyValue, clusteringKeyMetadata)),
        tableMetadata,
        timestamp);
  }

  @NonNull
  public static DeleteEvent createDeleteEventNoPk(
      Integer clusteringKeyValue, Column clusteringKeyMetadata, Table tableMetadata) {
    return createDeleteEvent(
        Collections.emptyList(),
        Collections.singletonList(cellValue(clusteringKeyValue, clusteringKeyMetadata)),
        tableMetadata,
        0);
  }

  @NonNull
  public static DeleteEvent createDeleteEventNoCk(
      String partitionKeyValue, Column partitionKeyMetadata, Table tableMetadata) {
    return createDeleteEvent(
        Collections.singletonList(cellValue(partitionKeyValue, partitionKeyMetadata)),
        Collections.emptyList(),
        tableMetadata,
        0);
  }

  @NonNull
  public static DeleteEvent createDeleteEvent(
      List<CellValue> partitionKeys,
      List<CellValue> clusteringKeys,
      Table tableMetadata,
      long timestamp) {
    return new DeleteEvent() {
      @Override
      public List<CellValue> getPartitionKeys() {
        return partitionKeys;
      }

      @Override
      public List<CellValue> getClusteringKeys() {
        return clusteringKeys;
      }

      @Override
      public Table getTable() {
        return tableMetadata;
      }

      @Override
      public long getTimestamp() {
        return timestamp;
      }
    };
  }

  @NonNull
  public static Cell cell(Column columnMetadata, ByteBuffer columnValue) {
    return cell(columnMetadata, null, columnValue);
  }

  @NonNull
  public static Cell cell(Column columnMetadata, Object columnValue) {
    InetCodec INET_CODEC = new InetCodec();
    CqlDurationCodec CQL_DURATION_CODEC = new CqlDurationCodec();
    ByteBuffer byteBuffer = null;
    if (columnValue instanceof InetAddress) {
      byteBuffer = INET_CODEC.encode((InetAddress) columnValue, ProtocolVersion.DEFAULT);
    } else if (columnValue instanceof CqlDuration) {
      byteBuffer = CQL_DURATION_CODEC.encode((CqlDuration) columnValue, ProtocolVersion.DEFAULT);
    }
    return cell(columnMetadata, columnValue, byteBuffer);
  }

  @NonNull
  public static Cell cell(
      Column columnMetadata, @Nullable Object columnValue, @Nullable ByteBuffer byteBuffer) {
    return new Cell() {
      @Override
      public int getTTL() {
        return 0;
      }

      @Override
      public boolean isNull() {
        return false;
      }

      @Override
      public Column getColumn() {
        return columnMetadata;
      }

      @Override
      public ByteBuffer getValue() {
        return byteBuffer;
      }

      @Override
      public Object getValueObject() {
        return columnValue;
      }
    };
  }

  @NonNull
  public static CellValue cellValue(Object partitionKeyValue, Column columnMetadata) {
    return new CellValue() {
      @Override
      public ByteBuffer getValue() {
        return null;
      }

      @Override
      public Object getValueObject() {
        return partitionKeyValue;
      }

      @Override
      public Column getColumn() {
        return columnMetadata;
      }
    };
  }

  @NonNull
  public static Column partitionKey(String partitionKeyName) {
    return partitionKey(partitionKeyName, Column.Type.Text);
  }

  @NonNull
  public static Column partitionKey(String partitionKeyName, Column.ColumnType cqlType) {
    return ImmutableColumn.builder()
        .name(partitionKeyName)
        .kind(Column.Kind.PartitionKey)
        .type(cqlType)
        .build();
  }

  @NonNull
  public static Column clusteringKey(String clusteringKeyName, Column.ColumnType cqlType) {
    return ImmutableColumn.builder()
        .name(clusteringKeyName)
        .kind(Column.Kind.Clustering)
        .type(cqlType)
        .build();
  }

  @NonNull
  public static Column clusteringKey(String clusteringKeyName) {
    return clusteringKey(clusteringKeyName, Column.Type.Text);
  }

  @NonNull
  public static Column column(String columnName) {
    return column(columnName, Column.Type.Text);
  }

  @NonNull
  public static Column column(Column.ColumnType cqlType) {
    return column(COLUMN_NAME, cqlType);
  }

  @NonNull
  public static Column column(String columnName, Column.ColumnType cqlType) {
    return ImmutableColumn.builder()
        .name(columnName)
        .kind(Column.Kind.Regular)
        .type(cqlType)
        .build();
  }
}
