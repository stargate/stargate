package org.apache.cassandra.stargate.transport.internal;

import io.stargate.db.Persistence;
import io.stargate.db.Result;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.stargate.transport.ServerError;

/**
 * A temporary workaround to make the stargate coordinator wait for schema agreement on behalf of
 * the client. The problem with this is it will most likely make the client's request timeout if
 * schema agreement doesn't happen quickly.
 */
public class SchemaAgreement {

  /** The number of retries that will be performed if a schema disagreement is detected */
  private static final int SCHEMA_AGREEMENT_WAIT_RETRIES =
      Integer.getInteger("stargate.cql.schema.agreement.wait.retries", 1800);

  /** The amount of time (in milliseconds) between two consecutive schema agreement checks */
  private static final int SCHEMA_AGREEMENT_RETRIES_INTERVAL_MILLIS = 100;

  private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1);

  public static CompletableFuture<? extends Result> maybeWaitForAgreement(
      CompletableFuture<? extends Result> future, Persistence.Connection connection) {
    return future.thenCompose(
        (result) -> {
          CompletableFuture<? extends Result> resultFuture = future;
          if (result.kind == Result.Kind.SchemaChange) {
            CompletableFuture<Result> agreementFuture = new CompletableFuture<Result>();
            EXECUTOR.schedule(
                new SchemaAgreement.Handler(agreementFuture, result, connection),
                SCHEMA_AGREEMENT_RETRIES_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
            resultFuture = agreementFuture;
          }
          return resultFuture;
        });
  }

  private static class Handler implements Runnable {
    private final CompletableFuture<Result> future;
    private final Persistence.Connection connection;
    private final Result result;
    private int count;

    Handler(CompletableFuture<Result> future, Result result, Persistence.Connection connection) {
      this.future = future;
      this.connection = connection;
      this.result = result;
      this.count = SCHEMA_AGREEMENT_WAIT_RETRIES;
    }

    @Override
    public void run() {
      if (connection.isInSchemaAgreement()) {
        future.complete(result);
      } else if (count > 0) {
        count--;
        EXECUTOR.schedule(this, SCHEMA_AGREEMENT_RETRIES_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
      } else {
        future.completeExceptionally(
            new ServerError(
                "Failed to reach schema agreement after "
                    + (SCHEMA_AGREEMENT_WAIT_RETRIES * SCHEMA_AGREEMENT_RETRIES_INTERVAL_MILLIS)
                    + " milliseconds."));
      }
    }
  }
}
