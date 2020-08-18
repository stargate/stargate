/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.query;

import java.util.List;
import java.util.Objects;

import io.stargate.db.datastore.schema.Table;

public class UnsupportedQueryException extends IllegalArgumentException
{
    private String cql;
    private transient final Table table;
    private transient final Where<?> where;
    private transient final List<ColumnOrder> orders;

    public UnsupportedQueryException(String cql, Table table, Where<?> where,
                                     List<ColumnOrder> orders)
    {
        super(String.format("No table or view could satisfy the query '%s'", cql));
        this.cql = cql;
        this.table = table;
        this.where = where;
        this.orders = orders;
    }

    public String getCql()
    {
        return cql;
    }

    public Table getTable()
    {
        return table;
    }

    public Where<?> getWhere()
    {
        return where;
    }

    public List<ColumnOrder> getOrders()
    {
        return orders;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        UnsupportedQueryException that = (UnsupportedQueryException) o;
        return Objects.equals(cql, that.cql);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cql);
    }
}
