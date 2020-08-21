/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface NWhere<T> extends Where<T>
{
    List<Where<T>> children();

    /**
     * Deep search of an expression for any match to the predicate
     */
    default boolean anyNodeMatch(Predicate<Where<?>> predicate)
    {
        if (predicate.test(this))
        {
            return true;
        }
        return children().stream().anyMatch(c -> c.anyNodeMatch(predicate));
    }

    /**
     * Deep search of an expression for all leaf nodes matching predicate
     */
    default boolean allLeafNodesMatch(Predicate<Where<?>> predicate)
    {
        return children().stream().allMatch(c -> c.allLeafNodesMatch(predicate));
    }

    /**
     * Deep search of an expression for any match to the predicate
     */
    default void forEachNode(Consumer<Where<?>> consumer)
    {
        consumer.accept(this);
        children().stream().forEach(c -> c.forEachNode(consumer));
    }

    default <T> List<T> getAllNodesOfType(Class<T> clazz)
    {
        List<T> matching = new ArrayList<>();
        if (clazz.isInstance(this))
        {
            matching.add((T) this);
        }

        if (this instanceof NWhere)
        {
            for (Where w : ((NWhere<?>) this).children())
            {
                matching.addAll(w.getAllNodesOfType(clazz));
            }
        }

        return matching;
    }
}
