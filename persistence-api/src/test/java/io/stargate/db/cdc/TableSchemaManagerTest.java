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
package io.stargate.db.cdc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import io.stargate.db.cdc.SchemaAwareCDCProducer.TableSchemaManager;
import io.stargate.db.schema.ImmutableTable;
import io.stargate.db.schema.Table;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class TableSchemaManagerTest {
  @Test
  public void shouldNotCallCreateWhenTheVersionMatches() {
    AtomicInteger counter = new AtomicInteger();
    TableSchemaManager m = newInstance(counter);
    Table table = newTable(1);

    m.ensureCreated(table);
    assertThat(counter.get()).isEqualTo(1);
    m.ensureCreated(table);
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void shouldNotCallCreateWhenTheVersionMatchesInParallel() {
    AtomicInteger counter = new AtomicInteger();
    TableSchemaManager m = newInstance(counter);
    Table table = newTable(1);

    invokeParallel(() -> m.ensureCreated(table), 8);
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void shouldCallCreateWhenTableHashCodeCollidesButNotEqual() {
    // hash code of the Aa and BB is the same
    Table table1 = ImmutableTable.builder().name("Aa").keyspace("ks1").build();
    Table table2 = ImmutableTable.builder().name("BB").keyspace("ks1").build();

    assertThat(table1.hashCode()).isEqualTo(table2.hashCode());

    AtomicInteger counter = new AtomicInteger();
    TableSchemaManager m = newInstance(counter);
    m.ensureCreated(table1);
    m.ensureCreated(table2);

    // Create was invoked once per table
    assertThat(counter.get()).isEqualTo(2);
  }

  private static TableSchemaManager newInstance(AtomicInteger counter) {
    return new TableSchemaManager(
        (a) -> {
          counter.incrementAndGet();
          CompletableFuture<Void> f = new CompletableFuture<>();
          // Complete async
          ExecutorService executor = Executors.newFixedThreadPool(1);
          executor.submit(() -> f.complete(null));
          return f;
        });
  }

  /** Creates a new table with a hash code based on the {@code id} parameter. */
  private static Table newTable(int id) {
    // Take advantage of ImmutableTable precomputed hashcode
    return ImmutableTable.builder().name("sample_table").keyspace("k" + id).build();
  }

  public static <T> List<T> invokeParallel(Callable<T> task, int times) {
    return invokeParallel(task, times, times);
  }

  public static <T> List<T> invokeParallel(Callable<T> task, int times, int nThreads) {
    ExecutorService executor = Executors.newFixedThreadPool(nThreads);
    List<Callable<T>> list = Collections.nCopies(times, task);

    List<Future<T>> futures = null;
    try {
      futures = executor.invokeAll(list);
    } catch (InterruptedException e) {
      fail("Tasks interrupted");
    }

    return futures.stream()
        .map(
            tFuture -> {
              try {
                return tFuture.get();
              } catch (Exception e) {
                throw new RuntimeException("Future could not be got", e);
              }
            })
        .collect(Collectors.toList());
  }
}
