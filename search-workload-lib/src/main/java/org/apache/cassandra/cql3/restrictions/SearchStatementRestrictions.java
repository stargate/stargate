/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.restrictions;

import com.datastax.bdp.search.solr.cql.NativeCQLSolrSelectStatement;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.WhereClause;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.schema.ColumnMetadata.Kind;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Expressed Solr queries as SELECT statements have to workaround C* SELECT validation.
 *
 * <p>- DseQueryHandler will parse it and inject custom StatementRestrictions. See: {@link
 * SearchRawStatement} - This class are the new restrictions that will be injected. Notice how IS
 * NOT NULL restrictions are treated. That is necessary as C* doesn't allow IS_NOT on SELECT, only
 * on Views - There is a custom DSESearchRawParsedWhereClause that takes care of that. Looks to C*
 * like a where clause without IS_NOT to pass validation and keeps those separate to be consumed
 * later at our discretion in {@link NativeCQLSolrSelectStatement} - That {@link SearchWhereClause}
 * contains C* relations that have been converted to {@link SearchSingleColumnRelation}
 * (DSESearchValidationUtils.fromCSingleColumnRelation). These just workaround several validations
 * C* relations had hardcoded. Notice SearchRestrictions.isSupportedBySolr2iIndex() where Operator
 * support is diverted so that Search and C* don't collide.
 *
 * <p>The result is a custom WhereClause with custom Relations that work with current C* code.
 */
public class SearchStatementRestrictions extends StatementRestrictions {
  private final Set<Restriction> isNotNullRestrictions;

  public boolean isPartitionKeyRoutable() {
    // hasOnlyEqualityRestrictions returns true for IN
    return (partitionKeyRestrictions.hasOnlyEqualityRestrictions()
            && !partitionKeyRestrictions.hasIN())
        || partitionKeyRestrictions.isOnToken();
  }

  public SearchStatementRestrictions(
      StatementType type,
      TableMetadata table,
      WhereClause whereClause,
      VariableSpecifications boundNames,
      boolean selectsOnlyStaticColumns,
      boolean allowFiltering,
      boolean forView) {
    super(type, table, whereClause, boundNames, selectsOnlyStaticColumns, allowFiltering, forView);
    SearchWhereClause searchWhereClause = (SearchWhereClause) whereClause;

    isNotNullRestrictions =
        searchWhereClause.isNotNullRelations.stream()
            .map(r -> r.toRestriction(table, boundNames))
            .collect(Collectors.toSet());
  }

  @Override
  protected void processPartitionKeyRestrictions(
      boolean hasQueriableIndex, boolean allowFiltering, boolean forView) {
    // If there are no partition restrictions or there's only token restriction, we have to set a
    // key range
    if (partitionKeyRestrictions.isOnToken()) isKeyRange = true;

    if (partitionKeyRestrictions.isEmpty()
        && partitionKeyRestrictions.hasUnrestrictedPartitionKeyComponents(table)) {
      isKeyRange = true;
      usesSecondaryIndexing = hasQueriableIndex;
    }

    if (partitionKeyRestrictions.needFiltering(table)) {
      isKeyRange = true;
      usesSecondaryIndexing = hasQueriableIndex;
    }
  }

  @Override
  public Restrictions getRestrictions(Kind kind) {
    return super.getRestrictions(kind);
  }

  public Set<Restriction> getIsNotNullRestrictions() {
    return isNotNullRestrictions;
  }

  @Override
  protected void validateSecondaryIndexSelections() {}
}
