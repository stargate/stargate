/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.dse.datastore;

import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.transport.messages.ResultMessage;

import io.stargate.db.datastore.ExecutionInfo;
import io.stargate.db.datastore.ResultSet;
import io.stargate.db.datastore.Row;
import io.stargate.db.datastore.common.InternalRow;
import io.stargate.db.datastore.schema.AbstractTable;
import com.google.common.base.Preconditions;
import hu.akarnokd.rxjava2.operators.FlowableTransformers;
import io.reactivex.Flowable;

/**
 * Represents a result set for {@link InternalDataStore}
 */
public class InternalResultSet implements ResultSet
{
    private final InternalDataStore.Executor executor;
    private final Flowable<Row> results;
    private AbstractTable table;

    private boolean waitedForSchemaAgreement;
    private ExecutionInfo executionInfo;
    private RowIterator<Row> currentIt;

    InternalResultSet(InternalDataStore.Executor executor, ResultMessage.Rows response,
                      boolean waitedForSchemaAgreement, ExecutionInfo executionInfo)
    {
        this.executor = executor;
        this.waitedForSchemaAgreement = waitedForSchemaAgreement;
        this.executionInfo = executionInfo;

        try
        {
            ColumnSpecification columnSpecification = response.result.metadata.requestNames().get(0);
            String ksName = columnSpecification.ksName;
            String cfName = columnSpecification.cfName;

            // Need to convert from virtual tables for system keyspace
            if (columnSpecification.ksName.equals("system_views"))
            {
                ksName = SchemaConstants.SYSTEM_KEYSPACE_NAME;
                if (columnSpecification.cfName.equals("local_node"))
                {
                    cfName = SystemKeyspace.LOCAL;
                }
                if (columnSpecification.cfName.equals("peer_nodes"))
                {
                    cfName = SystemKeyspace.PEERS;
                }
            }

            this.table = executor.schema().keyspace(ksName).table(cfName);
            if (table == null)
            {
                this.table = executor.schema().keyspace(ksName).materializedView(cfName);
            }

            Preconditions.checkState(table != null, "Table or view %s.%s was not found", columnSpecification.ksName,
                    columnSpecification.cfName);
        }
        catch (UnsupportedOperationException e)
        {
            // Probably a result list
        }
        results = Flowable.just(response.result)
                .compose(FlowableTransformers.expand(result -> fetchNext(result)))
                .flatMap(r -> Flowable.fromIterable(r.rows)
                        .map(row -> toRow(response.result.metadata.requestNames(), row)));
    }

    private Flowable<? extends org.apache.cassandra.cql3.ResultSet> fetchNext(org.apache.cassandra.cql3.ResultSet result)
    {
        QueryOptions.PagingOptions currentPagingOptions = executor.paging();

        // it's probably safe to look at the paging state regardless of the paging options, but
        // this makes it explicit that we won't retrieve any more pages if no paging options were set
        ByteBuffer nextPagingState = currentPagingOptions == null ? null : result.metadata.getPagingResult().state;
        this.executor.withPagingState(nextPagingState);

        if (nextPagingState == null || result.size() == currentPagingOptions.pageSize().inRows())
        {
            return Flowable.empty();
        }

        return executor.withPagingState(nextPagingState).query().map(r -> ((ResultMessage.Rows) r).result).toFlowable();
    }

    private Row toRow(List<ColumnSpecification> names, List<ByteBuffer> columns)
    {
        return new InternalRow(table, new UntypedResultSet.Row(names, columns));
    }

    /**
     * @return The size of the entire result set not including the elements that were already fetched
     */
    @Override
    public int size()
    {
        return currentIterator().size();
    }

    /**
     * @return The next {@link Row} if available.
     */
    @Override
    public Row one()
    {
        return currentIterator().next();
    }

    /**
     * @return all the rows. Warning, with paging this could potentially return many rows, enough to cause an OOM.
     * <p>
     * TODO: perhaps we should only return the first page here? I copied the behavior from ExternalDseResultSet
     * but it looks dangerous to me
     */
    @Override
    public List<Row> rows()
    {
        List<Row> rows = new ArrayList<>(currentIterator().size());
        currentIterator().forEachRemaining(rows::add);
        return rows;
    }

    @NotNull
    @Override
    public Iterator<Row> iterator()
    {
        return currentIterator();
    }

    @NotNull
    private RowIterator<Row> currentIterator()
    {
        if (null == currentIt)
        {
            currentIt = new RowIterator<>(TPCUtils.blockingGet(results.count()).intValue(),
                    results.blockingIterable().iterator());
        }
        return currentIt;
    }

    @Override
    public boolean isEmpty()
    {
        return TPCUtils.blockingGet(results.isEmpty());
    }

    @Override
    public boolean waitedForSchemaAgreement()
    {
        return waitedForSchemaAgreement;
    }

    @Override
    public ExecutionInfo getExecutionInfo()
    {
        return executionInfo;
    }

    public ByteBuffer getPagingState() {
        return this.executor.paging().state();
    }

    private class RowIterator<E> implements Iterator<E>
    {
        private int index;
        private int size;
        private Iterator<E> it;

        private RowIterator(int size, Iterator<E> it)
        {
            this.it = it;
            this.size = size;
        }

        @Override
        public boolean hasNext()
        {
            return it.hasNext();
        }

        @Override
        public E next()
        {
            E next = it.next();
            index++;
            return next;
        }

        @Override
        public void remove()
        {
            it.remove();
            index--;
        }

        public int size()
        {
            return size - index;
        }
    }
}
