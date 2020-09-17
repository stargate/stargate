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
package io.stargate.db.datastore.query;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

public interface NWhere<T> extends Where<T> {
  List<Where<T>> children();

  /** Deep search of an expression for any match to the predicate */
  default boolean anyNodeMatch(Predicate<Where<?>> predicate) {
    if (predicate.test(this)) {
      return true;
    }
    return children().stream().anyMatch(c -> c.anyNodeMatch(predicate));
  }

  /** Deep search of an expression for all leaf nodes matching predicate */
  default boolean allLeafNodesMatch(Predicate<Where<?>> predicate) {
    return children().stream().allMatch(c -> c.allLeafNodesMatch(predicate));
  }

  /** Deep search of an expression for any match to the predicate */
  default void forEachNode(Consumer<Where<?>> consumer) {
    consumer.accept(this);
    children().stream().forEach(c -> c.forEachNode(consumer));
  }

  default <T> List<T> getAllNodesOfType(Class<T> clazz) {
    List<T> matching = new ArrayList<>();
    if (clazz.isInstance(this)) {
      matching.add((T) this);
    }

    if (this instanceof NWhere) {
      for (Where w : ((NWhere<?>) this).children()) {
        matching.addAll(w.getAllNodesOfType(clazz));
      }
    }

    return matching;
  }
}
