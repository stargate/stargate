/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class Schema
{
    private static final Keyspace ANONYMOUS = ImmutableKeyspace.builder().name("<anonymous>").build();

    public abstract Set<Keyspace> keyspaces();

    @Value.Lazy
    Map<String, Keyspace> keyspaceMap()
    {
        return keyspaces().stream().collect(Collectors.toMap(Keyspace::name,
                Function.identity()));
    }

    public Keyspace keyspace(String name)
    {
        if (name == null)
        {
            return ANONYMOUS;
        }

        return keyspaceMap().get(name);
    }

    public List<String> keyspaceNames()
    {
        return keyspaces().stream()
                .map(k -> k.name()).sorted().collect(Collectors.toList());
    }

    public static Schema create(Set<Keyspace> keyspaces)
    {
        return ImmutableSchema.builder().addAllKeyspaces(keyspaces).build();
    }

    public static SchemaBuilder build()
    {
        return new SchemaBuilder(Optional.empty());
    }

    public static SchemaBuilder build(Consumer<Schema> callback)
    {
        return new SchemaBuilder(Optional.of(callback));
    }

    @Override
    public String toString()
    {
        return keyspaces().stream().map(Keyspace::toString).collect(Collectors.joining("\n"));
    }
}
