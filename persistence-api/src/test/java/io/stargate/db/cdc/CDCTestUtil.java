package io.stargate.db.cdc;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public final class CDCTestUtil {
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
