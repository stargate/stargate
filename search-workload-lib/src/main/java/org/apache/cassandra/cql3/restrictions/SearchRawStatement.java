/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.restrictions;

import com.datastax.bdp.search.solr.cql.NativeCQLSolrSelectStatement;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public class SearchRawStatement extends SelectStatement.Raw {
  public SearchRawStatement(
      QualifiedName cfName,
      SelectStatement.Parameters parameters,
      List<RawSelector> selectClause,
      SearchWhereClause whereClause,
      Term.Raw limit,
      Term.Raw perPartitionLimit) {
    super(cfName, parameters, selectClause, whereClause, limit, perPartitionLimit);
  }

  /**
   * Gets a parsed SELECT and injects/overloads/workarounds validation to make {@link
   * NativeCQLSolrSelectStatement} possible
   */
  public static SearchRawStatement fromRawStatement(SelectStatement.Raw statement) {
    QualifiedName cfName = new QualifiedName();
    cfName.setName(statement.name(), true);
    cfName.setKeyspace(statement.keyspace(), true);

    // Pretend statement always came with ALLOW FILTERING
    SelectStatement.Parameters origParams = statement.parameters;
    SelectStatement.Parameters newParams =
        new SelectStatement.Parameters(
            origParams.orderings,
            origParams.groups,
            origParams.isDistinct,
            true,
            origParams.isJson);

    SearchWhereClause searchWhereClause = SearchWhereClause.fromWhereClause(statement.whereClause);
    return new SearchRawStatement(
        cfName,
        newParams,
        statement.selectClause,
        searchWhereClause,
        statement.limit,
        statement.perPartitionLimit);
  }

  // Here we inject our own restrictions
  @Override
  protected StatementRestrictions prepareRestrictions(
      TableMetadata metadata,
      VariableSpecifications boundNames,
      boolean selectsOnlyStaticColumns,
      boolean forView)
      throws InvalidRequestException {
    return new SearchStatementRestrictions(
        StatementType.SELECT,
        metadata,
        whereClause,
        boundNames,
        selectsOnlyStaticColumns,
        parameters.allowFiltering,
        forView);
  }

  // Here we deactivate all the ORDER BY checks
  @Override
  protected void verifyOrderingIsAllowed(StatementRestrictions restrictions)
      throws InvalidRequestException {}

  @Override
  protected Comparator<List<ByteBuffer>> getOrderingComparator(
      Selection selection,
      StatementRestrictions restrictions,
      Map<ColumnMetadata, Boolean> orderingColumns)
      throws InvalidRequestException {
    return null;
  }

  @Override
  protected boolean isReversed(
      TableMetadata table,
      Map<ColumnMetadata, Boolean> orderingColumns,
      StatementRestrictions restrictions)
      throws InvalidRequestException {
    return false;
  }
}
