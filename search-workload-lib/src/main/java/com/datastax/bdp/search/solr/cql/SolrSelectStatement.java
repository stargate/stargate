/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import com.google.common.base.Splitter;
import java.util.List;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.cql3.functions.AggregateFcts;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.solr.core.SolrCore;

public abstract class SolrSelectStatement {
  protected final SelectStatement statement;

  SolrSelectStatement(CQLStatement statement) {
    this.statement = (SelectStatement) statement;
  }

  abstract CqlSolrQueryRequest toSolrQueryRequest(
      SolrCore core, QueryOptions options, QueryState state);

  String getKeyspace() {
    return statement.keyspace();
  }

  String getColumnFamily() {
    return statement.table();
  }

  AbstractBounds<PartitionPosition> getPartitionKeyBounds(QueryOptions options) {
    return statement.getRestrictions().getPartitionKeyBounds(options);
  }

  boolean isCount() {
    boolean isCount = false;
    Iterable<org.apache.cassandra.cql3.functions.Function> functions = statement.getFunctions();
    if (functions != null) {
      for (org.apache.cassandra.cql3.functions.Function current : functions) {
        if (current.name().equals(AggregateFcts.countRowsFunction.name())) {
          isCount = true;
          break;
        }
      }
    }

    return isCount;
  }

  int getLimit(QueryOptions options) {
    return statement.getLimit(options);
  }

  TableMetadata getTableMetadata() {
    return statement.table;
  }

  boolean mustReturnResultsAsJson() {
    return statement.parameters.isJson;
  }

  List<ColumnSpecification> getSelectedColumns() {
    return statement.getSelection().getColumnMapping().getColumnSpecifications();
  }

  ResultMetadata getResultMetadata() {
    return statement.getSelection().getResultMetadata();
  }

  StatementRestrictions getRestrictions() {
    return statement.getRestrictions();
  }

  boolean hasWildcardSelection() {
    return statement.getSelection().isWildcard();
  }

  protected void validate(QueryOptions options) {
    if (options.getPagingOptions() != null && options.getPagingOptions().isContinuous()) {
      throw new InvalidRequestException("Solr queries do not allow continuous paging.");
    }

    if (statement.getPerPartitionLimit(options) != DataLimits.NO_ROWS_LIMIT) {
      throw new InvalidRequestException("Solr queries do not allow per partition limits.");
    }

    for (ColumnSpecification currentMapping :
        statement.getSelection().getColumnMapping().getMappings().keySet()) {
      for (ColumnMetadata currentAlias :
          statement.getSelection().getColumnMapping().getMappings().get(currentMapping)) {
        // Tuple subfields are mapped to the underlying column
        if (!Splitter.on('.')
            .splitToList(currentMapping.name.toString())
            .get(0)
            .equals(currentAlias.name.toString())) {
          throw new InvalidRequestException(
              "Aliased column names, UDFs and similar features are not available for Solr queries. "
                  + "Offending inputs are ["
                  + currentAlias.name.toString()
                  + "] and ["
                  + currentMapping.name.toString()
                  + "].");
        }
      }
    }
  }

  public String describeFilter(QueryState state, QueryOptions options) {
    return statement.getRowFilter(state, options).toString();
  }
}
