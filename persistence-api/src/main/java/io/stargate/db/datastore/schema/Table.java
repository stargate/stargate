/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.immutables.value.Value;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;

@Value.Immutable(prehash = true)
public abstract class Table extends AbstractTable
{
    private static final long serialVersionUID = 5913904335622827700L;

    public abstract List<Index> indexes();

    @Value.Lazy
    Map<String, Index> indexMap()
    {
        return indexes().stream().collect(Collectors.toMap(Index::name,
                Function.identity()));
    }

    public Index index(String name)
    {
        return indexMap().get(name);
    }

    public static Table create(String keyspace, String name, List<Column> columns,
                               List<Index> indexes)
    {
        return ImmutableTable.builder().keyspace(keyspace).name(name).columns(columns)
                .indexes(indexes).build();
    }

    public static Table reference(String keyspace, String name)
    {
        return ImmutableTable.builder().keyspace(keyspace).name(name)
                .columns(ImmutableList.of()).indexes(ImmutableList.of()).build();
    }

    @Override
    public int priority()
    {
        return 0;
    }

    @Override
    public String toString()
    {
        StringBuilder tableBuilder = new StringBuilder();
        String name = name();
        tableBuilder.append("Table '").append(name).append("'");
        tableBuilder.append(":\n");
        tableBuilder.append(columns().stream()
                .map(c -> "    " + c.name() + " " + c.type()
                        + (c.kind() == Column.Kind.Regular ? "" : (" " + c.kind())))
                .collect(Collectors.joining("\n")));
        if (!indexes().isEmpty())
        {
            tableBuilder.append("\n");
            tableBuilder.append("\n  Table '").append(name).append("' indexes:\n");
            tableBuilder.append(indexes().stream().map(idx -> "    " + idx.toString())
                    .collect(Collectors.joining("\n")));
            tableBuilder.append("\n");
        }
        tableBuilder.append("\n");
        return tableBuilder.toString();
    }

    @Override
    public String indexTypeName()
    {
        return "Table: " + name();
    }
}
