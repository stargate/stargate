/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package io.stargate.db.datastore.schema;

import java.util.List;
import java.util.OptionalLong;

import io.stargate.db.datastore.query.ColumnOrder;
import io.stargate.db.datastore.query.WhereCondition;

public interface Index extends SchemaEntity
{
    /**
     * @return true if the query can be supported by the index
     */
    boolean supports(List<Column> select, List<WhereCondition<?>> conditions, List<ColumnOrder> orders,
                     OptionalLong limit);

    /**
     * @return The priority of this class of index where a lower value indicates a higher priority.
     */
    int priority();

    String indexTypeName();
}
