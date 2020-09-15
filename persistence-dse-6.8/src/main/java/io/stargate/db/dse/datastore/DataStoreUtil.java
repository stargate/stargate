package io.stargate.db.dse.datastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.schema.SchemaConstants;
import org.javatuples.Pair;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableMap;
import io.stargate.db.datastore.common.util.ColumnUtils;
import io.stargate.db.datastore.schema.CollectionIndexingType;
import io.stargate.db.datastore.schema.Column;
import io.stargate.db.datastore.schema.ImmutableCollectionIndexingType;
import io.stargate.db.datastore.schema.ImmutableColumn;
import io.stargate.db.datastore.schema.ImmutableUserDefinedType;

import static java.util.stream.Collectors.toList;

public class DataStoreUtil
{
    private static final Map<Class<? extends AbstractType>, Column.Type> TYPE_MAPPINGS;

    static
    {
        Map<Class<? extends AbstractType>, Column.Type> types = new HashMap<>();
        Arrays.asList(Column.Type.values()).forEach(ct ->
        {
            if (ct != Column.Type.Tuple && ct != Column.Type.List && ct != Column.Type.Map && ct != Column.Type.Set
                    && ct != Column.Type.UDT)
            {
                types.put(ColumnUtils.toInternalType(ct).getClass(), ct);
            }
        });
        TYPE_MAPPINGS = ImmutableMap.copyOf(types);
    }

    /**
     * Using a secondary index on a column X where X is a map and depending on the type of indexing, such as KEYS(X) /
     * VALUES(X) / ENTRIES(X), the actual column name will be wrapped, and we're trying to extract that column name
     * here.
     *
     * @param secondaryIndexTargetColumn The target column
     * @return A {@link Pair} containing the actual target column name as first parameter and the
     * {@link CollectionIndexingType} as the second parameter.
     */
    public static Pair<String, CollectionIndexingType> extractTargetColumn(String secondaryIndexTargetColumn)
    {
        if (null == secondaryIndexTargetColumn)
        {
            return new Pair<>(secondaryIndexTargetColumn, ImmutableCollectionIndexingType.builder().build());
        }
        if (secondaryIndexTargetColumn.startsWith("values("))
        {
            return new Pair<>(secondaryIndexTargetColumn.replace("values(", "").replace(")", "").replace("\"", ""),
                    ImmutableCollectionIndexingType.builder().indexValues(true).build());
        }
        if (secondaryIndexTargetColumn.startsWith("keys("))
        {
            return new Pair<>(secondaryIndexTargetColumn.replace("keys(", "").replace(")", "").replace("\"", ""),
                    ImmutableCollectionIndexingType.builder().indexKeys(true).build());
        }
        if (secondaryIndexTargetColumn.startsWith("entries("))
        {
            return new Pair<>(secondaryIndexTargetColumn.replace("entries(", "").replace(")", "").replace("\"", ""),
                    ImmutableCollectionIndexingType.builder().indexEntries(true).build());
        }
        if (secondaryIndexTargetColumn.startsWith("full("))
        {
            return new Pair<>(secondaryIndexTargetColumn.replace("full(", "").replace(")", "").replace("\"", ""),
                    ImmutableCollectionIndexingType.builder().indexFull(true).build());
        }
        return new Pair<>(secondaryIndexTargetColumn.replaceAll("\"", ""),
                ImmutableCollectionIndexingType.builder().build());
    }

    /**
     * When using {@link CqlCollection#entryEq} we're actually passing a {@link Pair} where the first value is the map
     * key and the second value is map value and we need to extract the second value.
     */
    public static Object maybeExtractParameterIfMapPair(Object parameter)
    {
        return parameter instanceof Pair ? ((Pair) parameter).getValue1() : parameter;
    }

    public static Column.ColumnType getTypeFromInternal(AbstractType abstractType)
    {
        if (abstractType instanceof ReversedType)
        {
            return getTypeFromInternal(((ReversedType) abstractType).baseType);
        }
        if (abstractType instanceof MapType)
        {
            return Column.Type.Map.of(getTypeFromInternal(((MapType) abstractType).getKeysType()),
                    getTypeFromInternal(((MapType) abstractType).getValuesType()))
                    .frozen(!abstractType.isMultiCell());
        }
        else if (abstractType instanceof SetType)
        {
            return Column.Type.Set.of(getTypeFromInternal(((SetType) abstractType).getElementsType()))
                    .frozen(!abstractType.isMultiCell());
        }
        else if (abstractType instanceof ListType)
        {
            return Column.Type.List.of(getTypeFromInternal(((ListType) abstractType).getElementsType()))
                    .frozen(!abstractType.isMultiCell());
        }
        else if (abstractType.getClass().equals(TupleType.class))
        {
            TupleType tupleType = ((TupleType) abstractType);
            return Column.Type.Tuple.of(((TupleType) abstractType).subTypes().stream().map(t -> getTypeFromInternal(t))
                    .toArray(Column.ColumnType[]::new))
                    .frozen(!tupleType.isMultiCell());
        }
        else if (abstractType.getClass().equals(UserType.class))
        {
            UserType udt = (UserType) abstractType;
            return ImmutableUserDefinedType.builder().keyspace(udt.keyspace).name(udt.getNameAsString())
                    .addAllColumns(getUDTColumns(udt))
                    .build()
                    .frozen(!udt.isMultiCell());
        }

        Column.Type type = TYPE_MAPPINGS.get(abstractType.getClass());
        Preconditions.checkArgument(type != null, "Unknown type mapping for %s", abstractType.getClass());
        return type;
    }

    public static List<Column> getUDTColumns(org.apache.cassandra.db.marshal.UserType userType)
    {
        List<Column> columns = new ArrayList<>(userType.fieldTypes().size());
        for (int i = 0; i < userType.fieldTypes().size(); i++)
        {
            columns.add(ImmutableColumn.builder().name(userType.fieldName(i).toString())
                    .type(getTypeFromInternal(userType.fieldType(i))).kind(Column.Kind.Regular).build());
        }
        return columns;
    }

    public static List<Column> getUDTColumns(UserDefinedType userType)
    {
        return userType.getFieldNames().stream().map(name ->
        {
            DataType type = userType.getFieldTypes().get(userType.firstIndexOf(name));
            return ImmutableColumn.builder().name(name.asInternal())
                    .type(getTypeFromDriver(type))
                    .kind(Column.Kind.Regular)
                    .build();
        }).collect(toList());
    }

    public static Column.ColumnType getTypeFromDriver(DataType type)
    {
        if (type.getProtocolCode() == ProtocolConstants.DataType.MAP)
        {
            com.datastax.oss.driver.api.core.type.MapType mapType = (com.datastax.oss.driver.api.core.type.MapType) type;
            return Column.Type.Map.of(getTypeFromDriver(mapType.getKeyType()),
                    getTypeFromDriver(mapType.getValueType())).frozen(mapType.isFrozen());
        }
        else if (type.getProtocolCode() == ProtocolConstants.DataType.SET)
        {
            com.datastax.oss.driver.api.core.type.SetType setType = (com.datastax.oss.driver.api.core.type.SetType) type;
            return Column.Type.Set.of(getTypeFromDriver(setType.getElementType()))
                    .frozen(setType.isFrozen());
        }
        else if (type.getProtocolCode() == ProtocolConstants.DataType.LIST)
        {
            com.datastax.oss.driver.api.core.type.ListType listType = (com.datastax.oss.driver.api.core.type.ListType) type;
            return Column.Type.List.of(getTypeFromDriver(listType.getElementType()))
                    .frozen(listType.isFrozen());
        }
        else if (type.getProtocolCode() == ProtocolConstants.DataType.TUPLE)
        {
            com.datastax.oss.driver.api.core.type.TupleType tupleType = (com.datastax.oss.driver.api.core.type.TupleType) type;
            return Column.Type.Tuple.of((tupleType).getComponentTypes().stream()
                    .map(DataStoreUtil::getTypeFromDriver)
                    .toArray(Column.ColumnType[]::new));
        }
        else if (type.getProtocolCode() == ProtocolConstants.DataType.CUSTOM)
        {
            // Each custom type is merely identified by the fully qualified class name that represents this type
            // server-side
            String customClassName = type.toString().replace("'", "");
            throw new IllegalStateException(String.format("Unknown CUSTOM type %s", customClassName));
        }
        else if (type.getProtocolCode() == ProtocolConstants.DataType.UDT)
        {
            com.datastax.oss.driver.api.core.type.UserDefinedType udt = (com.datastax.oss.driver.api.core.type.UserDefinedType) type;
            return ImmutableUserDefinedType.builder().keyspace(udt.getKeyspace().asInternal())
                    .name(udt.getName().asInternal())
                    .isFrozen(udt.isFrozen())
                    .addAllColumns(getUDTColumns(udt)).build();
        }

        return Column.Type.fromCqlDefinitionOf(type.toString()); // PrimitiveType
    }

    public static String mvIndexNotReadyMessage(String keyspaceName, String mvName)
    {
        return String.format("Materialized view '%s' is not ready to be queried as it still might be being built.\n" +
                        "You can check its progress with: 'nodetool viewbuildstatus %s %s'",
                mvName, keyspaceName, mvName);
    }

    public static String secondaryIndexNotReadyMessage(String keyspaceName, String indexName)
    {
        String cql = String.format("SELECT index_name FROM %s.\"%s\" WHERE table_name=%s AND index_name=%s",
                SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.BUILT_INDEXES, keyspaceName, indexName);
        return String.format("Secondary index '%s' is not ready to be queried as it still might be being built.\n" +
                "You can check its progress with: '%s'", indexName, cql);
    }
}
