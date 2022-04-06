/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.restrictions;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

import com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction.*;
import org.apache.cassandra.db.MultiCBuilder;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class SearchRestrictions {
  /*
   * C* index selection relies on AbstractSolrSecondaryIndex.supportsExpression(). This interferes with expressed solr queries
   * so we need to sidestep into our own Operator support check
   */
  public static boolean isSupportedBySolr2iIndex(
      Index index, ColumnMetadata columnDef, Operator operator) {
    boolean res = false;
    if (index instanceof Cql3SolrSecondaryIndex) {
      Cql3SolrSecondaryIndex solrIndex = (Cql3SolrSecondaryIndex) index;
      res = solrIndex.supportsOperator(columnDef, operator);
    }

    return res;
  }

  public static class SearchLikeRestriction extends LikeRestriction {
    public SearchLikeRestriction(LikeRestriction likeRes) {
      super(likeRes.columnDef, likeRes.operator, likeRes.value);
    }

    // Copy-paste from the parent removing the validation
    @Override
    public void addToRowFilter(
        RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      Pair<Operator, ByteBuffer> operation = makeSpecific(value.bindAndGet(options));
      filter.add(columnDef, operation.left, operation.right);
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, operator);
    }
  }

  public static class SearchIsNotNullRestriction extends IsNotNullRestriction {
    public SearchIsNotNullRestriction(IsNotNullRestriction restriction) {
      super(restriction.columnDef);
    }

    @Override
    public void addToRowFilter(
        RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      filter.add(columnDef, Operator.IS_NOT, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.IS_NOT);
    }
  }

  public static class SearchContainsRestriction extends ContainsRestriction {
    public SearchContainsRestriction(ContainsRestriction containsRes) {
      super(containsRes.columnDef);
      ContainsRestriction.copyKeysAndValues(containsRes, this);
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      boolean supported = false;

      if (numberOfValues() > 0)
        supported |=
            SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.CONTAINS);

      if (numberOfKeys() > 0)
        supported |=
            SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.CONTAINS_KEY);

      if (numberOfEntries() > 0)
        supported |= SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.EQ);

      return supported;
    }
  }

  public static class SearchEQRestriction extends EQRestriction {
    public SearchEQRestriction(EQRestriction eqRes) {
      super(eqRes.columnDef, eqRes.value);
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.EQ);
    }
  }

  public static class SearchSliceRestriction extends SliceRestriction {
    public SearchSliceRestriction(SliceRestriction sliceRes) {
      super(sliceRes.columnDef, sliceRes.slice);
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.GT)
          && SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.GTE)
          && SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.LT)
          && SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.LTE);
    }

    // This restriction supported merging. The merged results needs to be converted to a Search one.
    @Override
    public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
      SingleRestriction merged = super.doMergeWith(otherRestriction);
      return merged instanceof SliceRestriction
          ? new SearchSliceRestriction((SliceRestriction) merged)
          : merged;
    }
  }

  public static class SearchInRestrictionWithValues extends InRestrictionWithValues {
    public SearchInRestrictionWithValues(InRestrictionWithValues inRestriction) {
      super(inRestriction.columnDef, inRestriction.values);
    }

    public SearchInRestrictionWithValues(ColumnMetadata columnDef, List<Term> values) {
      super(columnDef, values);
    }

    @Override
    public void addToRowFilter(
        RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      List<ByteBuffer> values = getValues(options);
      filter.add(columnDef, Operator.IN, new Lists.Value(values).get(options.getProtocolVersion()));
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.IN);
    }
  }

  public static class SearchInRestrictionWithMarkers extends InRestrictionWithMarker {
    public SearchInRestrictionWithMarkers(InRestrictionWithMarker inRestriction) {
      super(inRestriction.columnDef, inRestriction.marker);
    }

    public SearchInRestrictionWithMarkers(ColumnMetadata columnDef, AbstractMarker marker) {
      super(columnDef, marker);
    }

    @Override
    public void addToRowFilter(
        RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      List<ByteBuffer> values = getValues(options);
      filter.add(columnDef, Operator.IN, new Lists.Value(values).get(options.getProtocolVersion()));
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.IN);
    }
  }

  public static class SearchNEQRestriction extends EQRestriction {
    public SearchNEQRestriction(ColumnMetadata columnDef, Term value) {
      super(0, columnDef, value);
    }

    @Override
    MultiColumnRestriction toMultiColumnRestriction() {
      throw invalidRequest("NEQ multi column restrictions are not supported.", columnDef.name);
    }

    @Override
    public void addToRowFilter(
        RowFilter filter, IndexRegistry indexRegistry, QueryOptions options) {
      filter.add(columnDef, Operator.NEQ, value.bindAndGet(options));
    }

    @Override
    public MultiCBuilder appendTo(MultiCBuilder builder, QueryOptions options) {
      builder.addElementToAll(value.bindAndGet(options));
      checkFalse(
          builder.containsNull(), "Invalid null value in condition for column %s", columnDef.name);
      checkFalse(builder.containsUnset(), "Invalid unset value for column %s", columnDef.name);
      return builder;
    }

    @Override
    public String toString() {
      return String.format("NEQ(%s)", value);
    }

    @Override
    public SingleRestriction doMergeWith(SingleRestriction otherRestriction) {
      throw invalidRequest(
          "%s cannot be restricted by more than one relation if it includes a Not Equals",
          columnDef.name);
    }

    @Override
    protected boolean isSupportedBy(Index index) {
      return SearchRestrictions.isSupportedBySolr2iIndex(index, columnDef, Operator.NEQ);
    }
  }
}
