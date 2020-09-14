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
package io.stargate.db.datastore;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.stargate.db.datastore.schema.AbstractTable;
import io.stargate.db.datastore.schema.Column;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;

public interface Row
{

    boolean has(String column);

    default boolean has(Column column)
    {
        return has(column.name());
    }

    List<Column> columns();

    default Column column(String column)
    {
        Column c = table().column(column);
        Preconditions.checkArgument(c != null, "Unknown column '%s' on table '%s'", column, table().name());
        return c;
    }

    default int getInt(String column)
    {
        return getInt(column(column));
    }

    int getInt(Column column);

    default long getLong(String column)
    {
        return getLong(column(column));
    }

    long getLong(Column column);

    default long getCounter(String column)
    {
        return getCounter(column(column));
    }

    long getCounter(Column column);

    default BigDecimal getDecimal(String column)
    {
        return getDecimal(column(column));
    }

    BigDecimal getDecimal(Column column);

    default String getString(String column)
    {
        return getString(column(column));
    }

    String getString(Column column);

    default boolean getBoolean(String column)
    {
        return getBoolean(column(column));
    }

    boolean getBoolean(Column column);

    default byte getByte(String column)
    {
        return getByte(column(column));
    }

    byte getByte(Column column);

    default short getShort(String column)
    {
        return getShort(column(column));
    }

    short getShort(Column column);

    default double getDouble(String column)
    {
        return getDouble(column(column));
    }

    double getDouble(Column column);

    default float getFloat(String column)
    {
        return getFloat(column(column));
    }

    float getFloat(Column column);

    default ByteBuffer getBytes(String column)
    {
        return getBytes(column(column));
    }

    ByteBuffer getBytes(Column column);

    default InetAddress getInetAddress(String column)
    {
        return getInetAddress(column(column));
    }

    InetAddress getInetAddress(Column column);

    default UUID getUUID(String column)
    {
        return getUUID(column(column));
    }

    UUID getUUID(Column column);

    default BigInteger getVarint(String column)
    {
        return getVarint(column(column));
    }

    BigInteger getVarint(Column column);

    default Instant getTimestamp(String column)
    {
        return getTimestamp(column(column));
    }

    Instant getTimestamp(Column column);

    default LocalTime getTime(String column)
    {
        return getTime(column(column));
    }

    LocalTime getTime(Column column);

    default LocalDate getDate(String column)
    {
        return getDate(column(column));
    }

    LocalDate getDate(Column column);

    default CqlDuration getDuration(String column)
    {
        return getDuration(column(column));
    }

    CqlDuration getDuration(Column column);

    default <T> List<T> getList(String column)
    {
        return getList(column(column));
    }

    <T> List<T> getList(Column column);

    default <T> Set<T> getSet(String column)
    {
        return getSet(column(column));
    }

    <T> Set<T> getSet(Column column);

    default <K, V> Map<K, V> getMap(String column)
    {
        return getMap(column(column));
    }

    <K, V> Map<K, V> getMap(Column column);

    default TupleValue getTuple(String column)
    {
        return getTuple(column(column));
    }

    TupleValue getTuple(Column column);

    default UdtValue getUDT(String column)
    {
        return getUDT(column(column));
    }

    UdtValue getUDT(Column column);

    default void verifyColumn(Column column)
    {
        Preconditions.checkArgument(has(column), "Column '%s' is not defined in the Row's metadata.", column.name());
    }

    /**
     * @param column The name of the column
     * @return Figures out the type of the column by inspecting the metadata and returns it.
     * @throws RuntimeException if the column required is not defined in the metadata of the Row. Check if the Row contains the
     *                          column first with {@link #has(String)}.
     */
    default Object getValue(String column)
    {
        return getValue(column(column));
    }

    /**
     * @throws RuntimeException if the column required is not defined in the metadata of the Row. Check if the Row contains the
     *                          column first with {@link #has(Column)}.
     */
    default Object getValue(Column column)
    {
        try {
            switch (column.type().rawType())
            {
                case Uuid:
                    return getUUID(column);
                case Int:
                    return getInt(column);
                case Map:
                    return getMap(column);
                case Set:
                    return getSet(column);
                case Blob:
                    return getBytes(column);
                case Date:
                    return getDate(column);
                case Inet:
                    return getInetAddress(column);
                case List:
                    return getList(column);
                case Text:
                    return getString(column);
                case Time:
                    return getTime(column);
                case Ascii:
                    return getString(column);
                case Float:
                    return getFloat(column);
                case Tuple:
                    return getTuple(column);
                case Bigint:
                    return getLong(column);
                case Double:
                    return getDouble(column);
                case Varint:
                    return getVarint(column);
                case Boolean:
                    return getBoolean(column);
                case Counter:
                    return getCounter(column);
                case Decimal:
                    return getDecimal(column);
                case Tinyint:
                    return getByte(column);
                case Varchar:
                    return getString(column);
                case Duration:
                    return getDuration(column);
                case Smallint:
                    return getShort(column);
                case Timeuuid:
                    return getUUID(column);
                case Timestamp:
                    return getTimestamp(column);
            }
        } catch (IllegalArgumentException iae) {
            // If we get this far then we know the column exists on the table but if there is a null value then
            // org.apache.cassandra.cql3.UntypedResultSet.Row.has will still throw an IllegalArgumentException. This means
            // that nullable columns could prevent an entire row from being returned.
            return null;
        }
        throw new UnsupportedOperationException("Unknown type");
    }

    AbstractTable table();

    @Override
    String toString();
}
