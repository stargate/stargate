/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.restrictions;

import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.WhereClause;

/*
 * A C* WhereClause hiding IS NOT NULL relations to fool C* validation. Expressed Solr queries logic
 * will later consume the IS_NOT relations.
 */
public class SearchWhereClause extends WhereClause {
  final List<Relation> isNotNullRelations;

  private SearchWhereClause(Builder clauseBuilder, List<Relation> isNotNullRelations) {
    super(clauseBuilder);
    this.isNotNullRelations = isNotNullRelations;
  }

  public static SearchWhereClause fromWhereClause(WhereClause clause) {
    Builder newClauseBuilder = new Builder();
    List<Relation> isNotNullRelations = new ArrayList<>();

    for (Relation current : clause.relations) {
      Relation transformedCurrent =
          current instanceof SingleColumnRelation
              ? SearchSingleColumnRelation.fromCSingleColumnRelation(current)
              : current;
      if (current.operator().equals(Operator.IS_NOT)) {
        isNotNullRelations.add(transformedCurrent);
      } else {
        newClauseBuilder.add(transformedCurrent);
      }
    }

    return new SearchWhereClause(newClauseBuilder, isNotNullRelations);
  }
}
