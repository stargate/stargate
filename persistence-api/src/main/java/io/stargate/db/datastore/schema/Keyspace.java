/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.immutables.value.Value;
import org.javatuples.Pair;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;

@Value.Immutable(prehash = true)
public abstract class Keyspace implements SchemaEntity
{

    /**
     * This list is verified in a test against SystemInfo. We don't want to use this directly though for
     * ExternalDataStore as it shouldn't have dependencies on Cassandra's internal classes
     */
    public static final Set<String> SYSTEM_KEYSPACES = ImmutableSet.of(
            "system",
            "system_schema",
            "system_virtual_schema",
            "system_auth",
            "system_traces",
            "system_distributed",
            "system_backups",
            "dse_system",
            "dse_security",
            "dse_perf",
            "dse_insights_local",
            "dse_insights");

    private static final long serialVersionUID = -337891773492616286L;

    public abstract Set<Table> tables();

    public abstract List<UserDefinedType> userDefinedTypes();

    public abstract Map<String, String> replication();

    public abstract Optional<Boolean> durableWrites();

    @Value.Lazy
    Map<String, Table> tableMap()
    {
        return tables().stream().collect(Collectors.toMap(Table::name,
                Function.identity()));
    }

    public Table table(String name)
    {
        return tableMap().get(name);
    }

    @Value.Lazy
    Map<String, UserDefinedType> userDefinedTypeMap()
    {
        return userDefinedTypes().stream().collect(Collectors.toMap(UserDefinedType::name,
                Function.identity()));
    }

    public UserDefinedType userDefinedType(String typeName)
    {
        return userDefinedTypeMap().get(typeName);
    }

    @Value.Lazy
    Map<String, MaterializedView> materializedViewMap()
    {
        return tables().stream().flatMap(t -> t.indexes().stream()).filter(i -> i instanceof MaterializedView)
                .collect(Collectors.toMap(Index::name,
                        i -> (MaterializedView) i));
    }

    public MaterializedView materializedView(String name)
    {
        return materializedViewMap().get(name);
    }

    @Value.Lazy
    Map<String, SecondaryIndex> secondaryIndexMap()
    {
        return tables().stream().flatMap(t -> t.indexes().stream()).filter(i -> i instanceof SecondaryIndex)
                .collect(Collectors.toMap(Index::name,
                        i -> (SecondaryIndex) i));
    }

    public SecondaryIndex secondaryIndex(String name)
    {
        return secondaryIndexMap().get(name);
    }

    @Value.Lazy
    Map<Index, Table> reverseIndexMap()
    {
        return tables().stream().flatMap(t -> t.indexes().stream().map(i -> new Pair<>(t, i)))
                .collect(Collectors.toMap(Pair::getValue1, Pair::getValue0));
    }

    public Column getColumnFromTableOrIndex(String cf, String name)
    {
        // This is probably only a bit slower than constructing the column from the C* column
        Table table = table(cf);
        if (table != null)
        {
            Column c = table.column(name);

            if (c == null)
            {
                throw new IllegalArgumentException(
                        String.format("%s does not contain the requested column %s", table, name));
            }

            return c;
        }

        MaterializedView mv = materializedView(cf);
        if (mv != null)
        {
            Column c = mv.column(name);

            if (c == null)
            {
                throw new IllegalArgumentException(
                        String.format("materialized view %s does not contain the requested column %s", mv, name));
            }

            return c;
        }

        SecondaryIndex si = secondaryIndex(cf);
        if (si != null)
        {
            if (si.column().name().equals(name))
            {
                return si.column();
            }
            else
            {
                throw new RuntimeException("Secondary index does not contain the requested column");
            }
        }

        throw new RuntimeException("No table, MV, or secondary index matched the requested column");
    }

    public static Keyspace create(String name, Set<Table> tables,
                                  List<UserDefinedType> userDefinedTypes,
                                  Map<String, String> replication, Optional<Boolean> durableWrites)
    {
        return ImmutableKeyspace.builder().name(name).addAllTables(tables)
                .addAllUserDefinedTypes(userDefinedTypes)
                .putAllReplication(replication)
                .durableWrites(durableWrites)
                .build();
    }

    public static Keyspace reference(String name)
    {
        return ImmutableKeyspace.builder().name(name)
                .addAllTables(Collections.emptySet()).userDefinedTypes(Collections.emptyList())
                .putAllReplication(Collections.emptyMap())
                .durableWrites(Optional.empty())
                .build();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Keyspace '").append(name()).append("'\n");
        tables().forEach(table -> builder.append("  ").append(table.toString()));
        return builder.toString();
    }

    public AbstractTable tableOrMaterializedView(String name)
    {
        AbstractTable table = table(name);
        if (table == null)
        {
            return materializedView(name);
        }
        return table;
    }

    public Column.ColumnType typeOf(Object value)
    {
        if (value instanceof String)
        {
            return Column.Type.Varchar;
        }
        else if (value instanceof UUID)
        {
            return Column.Type.Uuid;
        }
        else if (value instanceof Date)
        {
            return Column.Type.Date;
        }
        else if (value instanceof ByteBuffer)
        {
            return Column.Type.Blob;
        }
        else if (value instanceof InetAddress)
        {
            return Column.Type.Inet;
        }
        else if (value instanceof TupleValue)
        {
            TupleValue tupleValue = (TupleValue) value;
            TupleType type = tupleValue.getType();
            List<DataType> componentTypes = type.getComponentTypes();
            Column.ColumnType[] values = new Column.ColumnType[componentTypes.size()];
            for (int count = 0; count < componentTypes.size(); count++)
            {
                values[count] = typeOf(tupleValue.getObject(count));
            }

            return Column.Type.Tuple.of(values);
        }
        else if (value instanceof UdtValue)
        {
            return userDefinedType(((UdtValue) value).getType().getName().asInternal());
        }
        else if (value instanceof Set && !((Set) value).isEmpty())
        {
            return Column.Type.Set.of(typeOf(((Set) value).iterator().next()).frozen());
        }
        else if (value instanceof List && !((List) value).isEmpty())
        {
            return Column.Type.List.of(typeOf(((List) value).get(0)).frozen());
        }
        else if (value instanceof Map && !((Map) value).isEmpty())
        {
            Map.Entry<?, ?> firstEntry = ((Map<?, ?>) value).entrySet().iterator().next();
            return Column.Type.Map.of(typeOf(firstEntry.getKey()).frozen(), typeOf(firstEntry.getValue()).frozen());
        }
        else
        {
            Column.Type type = Column.TYPE_MAPPING.get(value.getClass());
            if (type != null)
            {
                return type;
            }
        }
        throw new IllegalArgumentException("Could not find an appropriate CQL type for value '" + value + "'");
    }
}
