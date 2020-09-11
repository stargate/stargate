/*
 * Copyright The Stargate Authors
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
