/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore;

import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public interface ResultSet extends Iterable<Row>
{
    class Empty implements ResultSet
    {
        private boolean waitedForSchemaAgreement;

        private Empty(boolean waitedForSchemaAgreement)
        {

            this.waitedForSchemaAgreement = waitedForSchemaAgreement;
        }

        @Override
        public Iterator<Row> iterator()
        {
            return Collections.emptyIterator();
        }

        @Override
        public Row one()
        {
            throw new NoSuchElementException();
        }

        @Override
        public List<Row> rows()
        {
            return Collections.emptyList();
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public ByteBuffer getPagingState() {
            return null;
        }

        @Override
        public boolean waitedForSchemaAgreement()
        {
            return waitedForSchemaAgreement;
        }
    }

    ResultSet EMPTY_NO_SCHEMA_AGREEMENT = new Empty(false);
    ResultSet EMPTY_WITH_SCHEMA_AGREEMENT = new Empty(true);

    static ResultSet empty(boolean waitedForSchemaAgreement)
    {
        return waitedForSchemaAgreement ? EMPTY_WITH_SCHEMA_AGREEMENT : EMPTY_NO_SCHEMA_AGREEMENT;
    }

    static ResultSet empty()
    {
        return EMPTY_NO_SCHEMA_AGREEMENT;
    }

    @NotNull
    Iterator<Row> iterator();

    /**
     * @return the number of rows not yet iterated in the current page, without trying to fetch any additional pages.
     */
    int size();

    /**
     * @return the next row in the current page. This is the same as calling the {@link #iterator()}} next method, and
     * will attempt to fetch another page if the current page is exhausted.
     */
    Row one();

    /**
     * @return the remaining rows not yet iterated, in the current page or any other page not yet fetched. Use this
     * method with care as it can potentially retrieve many pages and return a lot of data.
     */
    List<Row> rows();

    /**
     * @return true if no more rows are available in the current page, without trying to fetch any additional pages.
     */
    boolean isEmpty();

    ByteBuffer getPagingState();

    /**
     * Returns true of this request waited for schema agreement.
     *
     * @return
     */
    default boolean waitedForSchemaAgreement()
    {
        return false;
    }

    default ExecutionInfo getExecutionInfo()
    {
        return ExecutionInfo.EMPTY;
    }
}
