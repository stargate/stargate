/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface Where<T>
{
    default boolean anyNodeMatch(Predicate<Where<?>> predicate)
    {
        return predicate.test(this);
    }

    /**
     * Deep search of an expression for all leaf nodes matching predicate
     */
    default boolean allLeafNodesMatch(Predicate<Where<?>> predicate)
    {
        return predicate.test(this);
    }

    /**
     * Deep search of an expression for any match to the predicate
     */
    default void forEachNode(Consumer<Where<?>> consumer)
    {
        consumer.accept(this);
    }

    default <T> List<T> getAllNodesOfType(Class<T> clazz)
    {
        if (clazz.isInstance(this))
        {
            return Collections.singletonList((T) this);
        }
        return Collections.emptyList();
    }
}
