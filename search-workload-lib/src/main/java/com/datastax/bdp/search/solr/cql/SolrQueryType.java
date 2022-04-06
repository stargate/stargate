/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import com.datastax.bdp.server.DseDaemon;
import java.util.List;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.SearchStatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.service.QueryState;

public enum SolrQueryType {
  NONE,
  EMBEDDED,
  NATIVE_CQL;

  public static SolrQueryType fromStatement(
      CQLStatement statement, QueryOptions queryOptions, QueryState state) {
    SolrQueryType type = NONE;

    if (statement instanceof SelectStatement) {
      if (((SelectStatement) statement).getRestrictions() instanceof SearchStatementRestrictions) {
        type = NATIVE_CQL;
      } else {
        List<Expression> expressions =
            ((SelectStatement) statement).getRowFilter(state, queryOptions).getExpressions();
        for (Expression expression : expressions) {
          if (DseDaemon.SOLR_QUERY_KEY.equals(expression.column().name.toString())) {
            type = EMBEDDED;
            if (expressions.size() != 1) {
              throw new IllegalArgumentException(
                  "Search queries must have only one index expression.");
            }
          }
        }
      }
    }

    return type;
  }
}
