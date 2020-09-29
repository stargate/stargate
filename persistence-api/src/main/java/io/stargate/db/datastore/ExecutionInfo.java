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
package io.stargate.db.datastore;

import io.stargate.db.schema.Index;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
public abstract class ExecutionInfo {
  public abstract String preparedCQL();

  public abstract long durationNanos();

  public abstract int count();

  public abstract Optional<Index> index();

  public static final ExecutionInfo EMPTY = create("<empty>", -1, Optional.empty());

  public static ExecutionInfo create(
      String preparedCQL, long durationNanos, Optional<Index> index) {
    return ImmutableExecutionInfo.builder()
        .preparedCQL(preparedCQL)
        .durationNanos(durationNanos)
        .count(1)
        .index(index)
        .build();
  }

  public static ExecutionInfo create(
      String preparedCQL, long durationNanos, int count, Optional<Index> index) {
    return ImmutableExecutionInfo.builder()
        .preparedCQL(preparedCQL)
        .durationNanos(durationNanos)
        .count(count)
        .index(index)
        .build();
  }

  public static ExecutionInfo combine(ExecutionInfo one, ExecutionInfo two) {
    return ImmutableExecutionInfo.builder()
        .preparedCQL(one.preparedCQL())
        .durationNanos(one.durationNanos() + two.durationNanos())
        .count(one.count() + two.count())
        .index(one.index())
        .build();
  }

  @Override
  public String toString() {
    long durationMillis = TimeUnit.MILLISECONDS.convert(durationNanos(), TimeUnit.NANOSECONDS);
    String duration = durationMillis > 0 ? "" + durationMillis : "< 1";
    String indexType = index().isPresent() ? " / Index type: " + index().get().indexTypeName() : "";
    return String.format(
        "%s / Duration: %s ms / Count: %s%s", preparedCQL(), duration, count(), indexType);
  }
}
