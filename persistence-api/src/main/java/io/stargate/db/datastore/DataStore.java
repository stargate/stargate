/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.cassandra.stargate.db.ConsistencyLevel;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import io.stargate.db.datastore.query.QueryBuilder;
import io.stargate.db.datastore.schema.Index;
import io.stargate.db.datastore.schema.Schema;

/**
 * This will be our interface in to the rest of DSE. By using this rather than calling static methods we have a fighting
 * chance of being able to unit test without starting C*.
 */
public interface DataStore
{
    /**
     * The fetch size for SELECT statements
     */
    int DEFAULT_ROWS_PER_PAGE = 1000;

    /**
     * Create a query using the DSL builder.
     */
    default QueryBuilder query()
    {
        return new QueryBuilder(this);
    }

    default CompletableFuture<ResultSet> query(String cql, Object... parameters)
    {
        return query(cql, Optional.empty(), parameters);
    }

    CompletableFuture<ResultSet> query(String cql, Optional<ConsistencyLevel> consistencyLevel, Object... parameters);

    default PreparedStatement prepare(String cql)
    {
        return prepare(cql, Optional.empty());
    }

    PreparedStatement prepare(String cql, Optional<Index> index);

    default CompletableFuture<ResultSet> processBatch(List<PreparedStatement> statements, List<Object[]> vals,
                                                      Optional<ConsistencyLevel> consistencyLevel)
    {
        throw new UnsupportedOperationException("Batching not supported on " + getClass().getSimpleName());
    }

    /**
     * Returns the current schema.
     *
     * @return The current schema.
     */
    Schema schema();

    /**
     * Create a listener that will be notified when there is a schema change.
     *
     * @param callback The callback to call.
     */
    void addSchemaChangeListener(Consumer<Schema> callback);

    /**
     * Wait for schema to agree across the cluster
     */
    default void waitForSchemaAgreement()
    {
        for (int count = 0; count < 100; count++)
        {
            if (isInSchemaAgreement())
            {
                return;
            }
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
        throw new IllegalStateException("Failed to reach schema agreement after 10 seconds.");
    }

    /**
     * Returns true if in schema agreement
     */
    boolean isInSchemaAgreement();

    class UnauthorizedException extends RuntimeException
    {
        private boolean rlac;

        private UnauthorizedException(boolean rlac, Throwable cause)
        {
            super(cause.getMessage(), cause);
            this.rlac = rlac;
        }

        private UnauthorizedException(boolean rlac)
        {
            this.rlac = rlac;
        }

        public static UnauthorizedException rlac(Throwable cause)
        {
            removeStackTracesRecursively(cause);
            return new UnauthorizedException(true, cause);
        }

        public static UnauthorizedException rbac(Throwable cause)
        {
            removeStackTracesRecursively(cause);
            return new UnauthorizedException(false, cause);
        }

        /**
         * Information may be leaked via stack trace, so we remove them.
         */
        public static void removeStackTracesRecursively(Throwable cause)
        {
            for (Throwable t = cause; t != null; t = t.getCause())
            {
                t.setStackTrace(new StackTraceElement[0]);
            }
        }

        public static UnauthorizedException rlac()
        {
            return new UnauthorizedException(true);
        }

        public static UnauthorizedException rbac()
        {
            return new UnauthorizedException(false);
        }

        public boolean isRlac()
        {
            return rlac;
        }

        public boolean isRbac()
        {
            return !rlac;
        }

        @Override
        public synchronized Throwable fillInStackTrace()
        {
            return this;
        }
    }
}
