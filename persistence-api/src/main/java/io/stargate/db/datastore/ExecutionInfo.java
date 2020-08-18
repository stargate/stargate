/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;

import io.stargate.db.datastore.schema.Index;

@Value.Immutable(prehash = true)
public abstract class ExecutionInfo
{
    public abstract String preparedCQL();

    public abstract long durationNanos();

    public abstract int count();

    public abstract Optional<Index> index();

    public static final ExecutionInfo EMPTY = create("<empty>", -1, Optional.empty());

    public static ExecutionInfo create(String preparedCQL, long durationNanos, Optional<Index> index)
    {
        return ImmutableExecutionInfo.builder().preparedCQL(preparedCQL)
                .durationNanos(durationNanos)
                .count(1)
                .index(index)
                .build();
    }

    public static ExecutionInfo create(String preparedCQL, long durationNanos, int count, Optional<Index> index)
    {
        return ImmutableExecutionInfo.builder().preparedCQL(preparedCQL)
                .durationNanos(durationNanos)
                .count(count)
                .index(index).build();
    }

    public static ExecutionInfo combine(ExecutionInfo one, ExecutionInfo two)
    {
        return ImmutableExecutionInfo.builder().preparedCQL(one.preparedCQL())
                .durationNanos(one.durationNanos() + two.durationNanos())
                .count(one.count() + two.count())
                .index(one.index())
                .build();
    }

    @Override
    public String toString()
    {
        long durationMillis = TimeUnit.MILLISECONDS.convert(durationNanos(), TimeUnit.NANOSECONDS);
        String duration = durationMillis > 0 ? "" + durationMillis : "< 1";
        String indexType = index().isPresent() ? " / Index type: " + index().get().indexTypeName() : "";
        return String.format("%s / Duration: %s ms / Count: %s%s", preparedCQL(), duration, count(), indexType);
    }
}
