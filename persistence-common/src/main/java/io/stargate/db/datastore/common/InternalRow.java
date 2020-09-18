/*
 * Copyright DataStax, Inc. and/or The Stargate Authors
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
package io.stargate.db.datastore.common;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.common.util.ColumnUtils;
import io.stargate.db.datastore.schema.AbstractTable;
import io.stargate.db.datastore.schema.Column;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class InternalRow implements Row {

  private final AbstractTable table;
  private final UntypedResultSet.Row internalRow;

  public InternalRow(AbstractTable table, UntypedResultSet.Row internalRow) {
    this.table = table;
    this.internalRow = internalRow;
  }

  @Override
  public boolean has(String column) {
    return internalRow.has(column);
  }

  @Override
  public List<Column> columns() {
    List<Column> columns = new ArrayList<>();
    internalRow.getColumns().forEach(def -> columns.add(table.column(def.name.toString())));
    return columns;
  }

  @Override
  public int getInt(Column column) {
    verifyColumn(column);
    return internalRow.getInt(column.name());
  }

  @Override
  public long getLong(Column column) {
    verifyColumn(column);
    return internalRow.getLong(column.name());
  }

  @Override
  public long getCounter(Column column) {
    verifyColumn(column);
    return CounterColumnType.instance.compose(internalRow.getBytes(column.name()));
  }

  @Override
  public BigDecimal getDecimal(Column column) {
    verifyColumn(column);
    return DecimalType.instance.compose(internalRow.getBytes(column.name()));
  }

  @Override
  public String getString(Column column) {
    verifyColumn(column);

    if (column.type() == Column.Type.Ascii) {
      return AsciiType.instance.compose(internalRow.getBytes(column.name()));
    } else {
      return internalRow.getString(column.name());
    }
  }

  @Override
  public boolean getBoolean(Column column) {
    verifyColumn(column);
    return internalRow.getBoolean(column.name());
  }

  @Override
  public byte getByte(Column column) {
    verifyColumn(column);
    return internalRow.getByte(column.name());
  }

  @Override
  public short getShort(Column column) {
    verifyColumn(column);
    return internalRow.getShort(column.name());
  }

  @Override
  public double getDouble(Column column) {
    verifyColumn(column);
    return internalRow.getDouble(column.name());
  }

  @Override
  public float getFloat(Column column) {
    verifyColumn(column);
    return FloatType.instance.compose(internalRow.getBytes(column.name()));
  }

  @Override
  public ByteBuffer getBytes(Column column) {
    verifyColumn(column);
    return ByteBuffer.wrap(ByteBufferUtil.getArray(internalRow.getBytes(column.name())));
  }

  @Override
  public InetAddress getInetAddress(Column column) {
    verifyColumn(column);
    return internalRow.getInetAddress(column.name());
  }

  @Override
  public UUID getUUID(Column column) {
    verifyColumn(column);
    return internalRow.getUUID(column.name());
  }

  @Override
  public BigInteger getVarint(Column column) {
    verifyColumn(column);
    return IntegerType.instance.compose(internalRow.getBytes(column.name()));
  }

  @Override
  public Instant getTimestamp(Column column) {
    verifyColumn(column);
    Date internalValue = TimestampType.instance.compose(internalRow.getBytes(column.name()));
    ;
    return (Instant) ColumnUtils.toExternalValue(column.type(), internalValue);
  }

  @Override
  public LocalTime getTime(Column column) {
    verifyColumn(column);
    Long internalRowValue = TimeType.instance.compose(internalRow.getBytes(column.name()));
    return (LocalTime) ColumnUtils.toExternalValue(column.type(), internalRowValue);
  }

  @Override
  public LocalDate getDate(Column column) {
    verifyColumn(column);
    Integer localDate = SimpleDateType.instance.compose(internalRow.getBytes(column.name()));
    return (LocalDate) ColumnUtils.toExternalValue(column.type(), localDate);
  }

  @Override
  public CqlDuration getDuration(Column column) {
    verifyColumn(column);
    org.apache.cassandra.cql3.Duration internalDuration =
        DurationType.instance.compose(internalRow.getBytes(column.name()));
    return (CqlDuration) ColumnUtils.toExternalValue(column.type(), internalDuration);
  }

  @Override
  public <T> List<T> getList(Column column) {
    verifyColumn(column);
    List list =
        internalRow.getList(
            column.name(), ColumnUtils.toInternalType(column.type().parameters().get(0)));
    return (List<T>) ColumnUtils.toExternalValue(column.type(), list);
  }

  @Override
  public <T> Set<T> getSet(Column column) {
    verifyColumn(column);
    Set set =
        internalRow.getSet(
            column.name(), ColumnUtils.toInternalType(column.type().parameters().get(0)));
    return (Set<T>) ColumnUtils.toExternalValue(column.type(), set);
  }

  @Override
  public <K, V> Map<K, V> getMap(Column column) {
    verifyColumn(column);
    Column.ColumnType keyType = column.type().parameters().get(0);
    Column.ColumnType valueType = column.type().parameters().get(1);
    return (Map<K, V>)
        ColumnUtils.toExternalValue(
            column.type(),
            internalRow.getMap(
                column.name(),
                ColumnUtils.toInternalType(keyType),
                ColumnUtils.toInternalType(valueType)));
  }

  public UdtValue getUDT(Column column) {
    verifyColumn(column);
    return (UdtValue)
        ColumnUtils.toExternalValue(
            column.type(),
            ColumnUtils.toInternalType(column.type()).compose(internalRow.getBytes(column.name())));
  }

  @Override
  public TupleValue getTuple(Column column) {
    verifyColumn(column);
    AbstractType internalType = ColumnUtils.toInternalType(column.type());
    return (TupleValue)
        ColumnUtils.toExternalValue(
            column.type(), internalType.compose(internalRow.getBytes(column.name())));
  }

  @Override
  public AbstractTable table() {
    return table;
  }

  @Override
  public String toString() {
    return internalRow.toString();
  }
}
