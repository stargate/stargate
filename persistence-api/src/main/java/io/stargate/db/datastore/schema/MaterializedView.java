/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.util.List;

import org.immutables.value.Value;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;

@Value.Immutable(prehash = true)
public abstract class MaterializedView extends AbstractTable implements Index
{
    private static final long serialVersionUID = -2999120284516448661L;

    public static MaterializedView create(String keyspace, String name, List<Column> columns)
    {
        columns.forEach(c ->
        {
            Preconditions.checkState(c.kind() != null, "Column reference may not be used %s", c.name());
        });
        return ImmutableMaterializedView.builder().keyspace(keyspace).name(name)
                .addAllColumns(columns).build();
    }

    public static MaterializedView reference(String keyspace, String name)
    {
        return ImmutableMaterializedView.builder().keyspace(keyspace).name(name)
                .addColumns().build();
    }

    @Override
    public int priority()
    {
        return 1;
    }

    @Override
    public String indexTypeName()
    {
        return "Materialized view";
    }
}
