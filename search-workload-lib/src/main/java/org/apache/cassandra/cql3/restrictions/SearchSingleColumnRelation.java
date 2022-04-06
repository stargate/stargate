/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.cql3.restrictions;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.Term.Raw;
import org.apache.cassandra.cql3.restrictions.SearchRestrictions.*;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction.*;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

public final class SearchSingleColumnRelation extends SingleColumnRelation {
  public static SearchSingleColumnRelation fromCSingleColumnRelation(Relation cassandraScrel) {
    SingleColumnRelation screl = (SingleColumnRelation) cassandraScrel;
    return fromCSingleColumnRelation(screl);
  }

  public static SearchSingleColumnRelation fromCSingleColumnRelation(
      SingleColumnRelation cassandraScrel) {
    return new SearchSingleColumnRelation(
        cassandraScrel.getEntity(),
        cassandraScrel.getMapKey(),
        cassandraScrel.operator(),
        cassandraScrel.getValue(),
        (List<Raw>) cassandraScrel.getInValues());
  }

  public SearchSingleColumnRelation(
      ColumnMetadata.Raw entity, Raw mapKey, Operator type, Raw value, List<Raw> inValues) {
    super(entity, mapKey, type, value, inValues);
  }

  @Override
  public Relation renameIdentifier(ColumnMetadata.Raw from, ColumnMetadata.Raw to) {
    Relation rel = super.renameIdentifier(from, to);

    if (rel instanceof SingleColumnRelation) {
      return SearchSingleColumnRelation.fromCSingleColumnRelation((SingleColumnRelation) rel);
    }

    return rel;
  }

  @Override
  public Restriction toRestriction(TableMetadata table, VariableSpecifications boundNames) {
    Restriction res = null;
    if (operator().equals(Operator.NEQ)) {
      res = newNEQRestriction(table, boundNames);
    } else {
      res = super.toRestriction(table, boundNames);

      if (res instanceof InRestrictionWithValues) {
        res = new SearchInRestrictionWithValues((InRestrictionWithValues) res);
      } else if (res instanceof InRestrictionWithMarker) {
        res = new SearchInRestrictionWithMarkers((InRestrictionWithMarker) res);
      } else if (res instanceof EQRestriction) {
        res = new SearchEQRestriction((EQRestriction) res);
      } else if (res instanceof SliceRestriction) {
        res = new SearchSliceRestriction((SliceRestriction) res);
      } else if (res instanceof ContainsRestriction) {
        res = new SearchContainsRestriction((ContainsRestriction) res);
      } else if (res instanceof LikeRestriction) {
        res = new SearchLikeRestriction((LikeRestriction) res);
      } else if (res instanceof IsNotNullRestriction) {
        res = new SearchIsNotNullRestriction((IsNotNullRestriction) res);
      }
    }

    return res;
  }

  /**
   * Returns the receivers for this relation.
   *
   * @param columnDef the column definition
   * @return the receivers for the specified relation.
   * @throws InvalidRequestException if the relation is invalid
   */
  @Override
  protected List<? extends ColumnSpecification> toReceivers(ColumnMetadata columnDef)
      throws InvalidRequestException {
    ColumnSpecification receiver = columnDef;

    checkFalse(
        isContainsKey() && !(receiver.type instanceof MapType),
        "Cannot use CONTAINS KEY on non-map column %s",
        receiver.name);
    checkFalse(
        isContains() && !(receiver.type.isCollection()),
        "Cannot use CONTAINS on non-collection column %s",
        receiver.name);
    checkFalse(
        !isContains() && (columnDef.type instanceof ListType || columnDef.type instanceof SetType),
        operator() + " predicates on collections (%s) are not supported",
        columnDef.name);

    // Map *entry* equality
    if (getMapKey() != null) {
      checkFalse(
          receiver.type instanceof ListType,
          "Indexes on list entries (%s[index] = value) are not currently supported.",
          receiver.name);
      checkTrue(
          receiver.type instanceof MapType, "Column %s cannot be used as a map", receiver.name);
      checkTrue(isEQ(), "Only EQ relations are supported on map entries");
    } else {
      checkFalse(
          !isContainsKey() && columnDef.type instanceof MapType,
          operator() + " predicates on maps (%s) are not supported",
          columnDef.name);
    }

    if (isContainsKey() || isContains()) {
      receiver = makeCollectionReceiver(receiver, isContainsKey());
    } else if (receiver.type.isCollection()) {
      List<ColumnSpecification> receivers = new ArrayList<>(2);
      receivers.add(makeCollectionReceiver(receiver, true));
      receivers.add(makeCollectionReceiver(receiver, false));
      return receivers;
    }

    return Collections.singletonList(receiver);
  }

  protected Restriction newNEQRestriction(TableMetadata table, VariableSpecifications boundNames)
      throws InvalidRequestException {
    ColumnMetadata columnDef = getEntity().prepare(table);
    Term term = toTerm(toReceivers(columnDef), getValue(), table.keyspace, boundNames);
    return new SearchRestrictions.SearchNEQRestriction(columnDef, term);
  }
}
