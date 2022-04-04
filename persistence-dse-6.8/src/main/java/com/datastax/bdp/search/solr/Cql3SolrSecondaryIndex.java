/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;

public class Cql3SolrSecondaryIndex extends AbstractSolrSecondaryIndex {

  public Cql3SolrSecondaryIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
    super(baseCfs, indexDef);
  }

  // DSE Search SELECT Restrictions will use this method instead of Index.supportsExpression()
  // as that one is used by C* to pick up indexes for the C* vanilla path and we need to support X
  // operators
  // but not get picked up as candidate index in C* at the same time.
  public boolean supportsOperator(ColumnMetadata column, Operator operator) {
    // There types are backed by a string in Solr and other scenarios so only some operators are
    // possible
    if ((column.type instanceof DecimalType
            || column.type instanceof IntegerType
            || column.type instanceof BooleanType)
        && !(operator.equals(Operator.EQ)
            || operator.isIN()
            || operator.equals(Operator.IS_NOT)
            || operator.equals(Operator.NEQ))) {
      if (column.type instanceof DecimalType || column.type instanceof IntegerType) {
        throw new InvalidRequestException(
            column.type
                + " is backed in Solr by a String so it doesn't support operator "
                + operator
                + ". Offending column: "
                + column.name.toString());
      }

      throw new InvalidRequestException(
          column.type
              + " and operator "
              + operator
              + " are not supported. Offending column: "
              + column.name.toString());
    }

    if (column.type instanceof MapType && operator.equals(Operator.CONTAINS)) {
      throw new InvalidRequestException(
          "CONTAINS on a map is not supported. Offending column: " + column.name.toString());
    }

    if ((column.type instanceof ListType || column.type instanceof SetType)
        && operator.equals(Operator.EQ)) {
      // EQ on *collection* (not on collection *entry*) has no Solr equivalent
      throw new InvalidRequestException(
          "Equality on a collections is not supported. Offending column: "
              + column.name.toString());
    }

    // LIKE is only allowed against text
    if (!(column.type instanceof UTF8Type)
        && !(column.type instanceof AsciiType)
        && (operator.equals(Operator.LIKE)
            || operator.equals(Operator.LIKE_MATCHES)
            || operator.equals(Operator.LIKE_CONTAINS)
            || operator.equals(Operator.LIKE_PREFIX)
            || operator.equals(Operator.LIKE_SUFFIX))) {
      throw new InvalidRequestException(
          "LIKE operator is only supported against text types. Offending column: "
              + column.name.toString());
    }

    // TODO-SEARCH
    // Tokenized text should only support LIKE and IS NOT NULL.
    /*if (!NativeCQLSolrSelectStatement.TOKENIZED_TEXT_ALLOWED_OPERATORS.contains(operator))
    {
      try (SolrCore core = getCore())
      {
        SchemaField field = core.getLatestSchema().getFieldOrNull(column.name.toString());
        assert field != null : "The field " + column.name.toString() + " could not be found in the schema.";
        if (field.getType() instanceof TextField)
        {
          throw new InvalidRequestException("Tokenized text only supports LIKE and IS NOT NULL operators. Offeding field: "
                  + column.name.toString() + " for operator: " + operator);
        }
      }
    }*/

    return true;
  }
}
