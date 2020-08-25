package org.apache.cassandra.stargate.transport.internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.stargate.transport.ServerError;

import io.stargate.db.Persistence;
import io.stargate.db.Result;


/**
 * A temporary workaround to make the coordinator wait for schema agreement on behalf of the
 * client. The problem with this is it will most likely make the client's request timeout if schema
 * agreement doesn't happen quickly.
 */
public class SchemaAgreement
{
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1);

    public static CompletableFuture<? extends Result> maybeWaitForAgreement(CompletableFuture<? extends Result> future, Persistence persistence)
    {
        return future.thenCompose((result) ->
        {
            CompletableFuture<? extends Result> resultFuture = future;
            if (result.kind == Result.Kind.SchemaChange)
            {
                CompletableFuture<Result> agreementFuture = new CompletableFuture<Result>();
                EXECUTOR.schedule(new SchemaAgreement.Handler(agreementFuture, result, persistence), 100, TimeUnit.MILLISECONDS);
                resultFuture = agreementFuture;
            }
            return resultFuture;
        });
    }

    private static class Handler implements Runnable
    {
        private final CompletableFuture<Result> future;
        private final Persistence persistence;
        private final Result result;
        private int count;

        Handler(CompletableFuture<Result> future, Result result, Persistence persistence)
        {
            this.future = future;
            this.persistence = persistence;
            this.result = result;
            this.count = 100;
        }

        @Override
        public void run()
        {
            if (persistence.isInSchemaAgreement())
            {
                future.complete(result);
            }
            else if (count > 0)
            {
                count--;
                EXECUTOR.schedule(this, 100, TimeUnit.MILLISECONDS);
            }
            else
            {
                future.completeExceptionally(new ServerError("Failed to reach schema agreement after 10 seconds."));
            }
        }
    }
}
