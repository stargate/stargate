/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import com.google.common.base.Preconditions;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.QueryState;
import org.apache.solr.core.SolrCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link SelectStatement} wrapper for selects containing a solr_query */
public class EmbeddedSolrQueryStatement extends SolrSelectStatement {
  protected static final Logger logger = LoggerFactory.getLogger(EmbeddedSolrQueryStatement.class);

  private final QueryParserFactory queryParserFactory = new QueryParserFactory();

  public EmbeddedSolrQueryStatement(
      CQLStatement statement, QueryOptions options, QueryState state) {
    super(statement);
    SolrQueryType type = SolrQueryType.fromStatement(statement, options, state);
    Preconditions.checkArgument(
        type == SolrQueryType.EMBEDDED,
        "The provided statement is not a valid CQL solr_query statement.");
  }

  @Override
  public CqlSolrQueryRequest toSolrQueryRequest(
      SolrCore core, QueryOptions options, QueryState state) {
    validate(options);
    RowFilter nonKeyFilter = this.statement.getRowFilter(state, options);
    String solrQueryString =
        UTF8Type.instance.compose(nonKeyFilter.getExpressions().get(0).getIndexValue());
    return parseSolrQueryColumn(core, solrQueryString);
  }

  @Override
  protected void validate(QueryOptions options) {
    super.validate(options);

    if (statement.getRestrictions().hasClusteringColumnsRestrictions()) {
      throw new IllegalArgumentException(
          "Search queries do not allow clustering column restrictions.");
    }
  }

  private CqlSolrQueryRequest parseSolrQueryColumn(SolrCore core, String query)
      throws InvalidRequestException {
    try {
      QueryParser queryParser = queryParserFactory.build(query);
      CqlSolrQueryRequest request = new CqlSolrQueryRequest(core, queryParser.parse(query));
      return request;
    } catch (Throwable ex) {
      throw new InvalidRequestException(ex.getMessage(), ex);
    }
  }
}
