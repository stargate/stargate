package io.stargate.db.cdc;

import static io.stargate.db.cdc.CDCTestUtil.invokeParallel;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.db.cdc.AsyncCDCProducer.TableSchemaManager;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.junit.jupiter.api.Test;

public class TableSchemaManagerTest {
  @Test
  public void shouldNotCallCreateWhenTheVersionMatches() {
    AtomicInteger counter = new AtomicInteger();
    TableSchemaManager m = newInstance(counter);
    TableMetadata table = newTable(1);

    m.ensureCreated(table);
    assertThat(counter.get()).isEqualTo(1);
    m.ensureCreated(table);
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void shouldNotCallCreateWhenTheVersionMatchesInParallel() {
    AtomicInteger counter = new AtomicInteger();
    TableSchemaManager m = newInstance(counter);
    TableMetadata table = newTable(1);

    invokeParallel(() -> m.ensureCreated(table), 8);
    assertThat(counter.get()).isEqualTo(1);
  }

  @Test
  public void shouldCallCreateWhenTableHashCodeCollidesButNotEqual() {
    Object o1 =
        new Object() {
          @Override
          public int hashCode() {
            return 1;
          }

          @Override
          public boolean equals(Object obj) {
            return false;
          }
        };
    Object o2 =
        new Object() {
          @Override
          public int hashCode() {
            return 1;
          }

          @Override
          public boolean equals(Object obj) {
            return false;
          }
        };
    TableMetadata table1 = newTable(o1);
    TableMetadata table2 = newTable(o2);

    assertThat(table1.getIdentity().hashCode()).isEqualTo(table2.getIdentity().hashCode());
    assertThat(table1.getIdentity()).isNotEqualTo(table2.getIdentity());

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

  private static TableMetadata newTable(Object identity) {
    TableMetadata t = mock(TableMetadata.class);
    when(t.getIdentity()).thenReturn(identity);
    return t;
  }
}
