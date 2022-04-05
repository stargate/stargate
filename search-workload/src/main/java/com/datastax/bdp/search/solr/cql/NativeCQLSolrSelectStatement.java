/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import com.datastax.bdp.search.solr.core.CassandraSolrConfig;
import com.datastax.bdp.search.solr.core.types.CassandraSolrTypeMapper;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Stream;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Lists.Value;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.restrictions.Restriction;
import org.apache.cassandra.cql3.restrictions.Restrictions;
import org.apache.cassandra.cql3.restrictions.SearchStatementRestrictions;
import org.apache.cassandra.cql3.restrictions.TokenRestriction;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.filter.RowFilter.Expression;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.marshal.CollectionType.Kind;
import org.apache.cassandra.db.marshal.datetime.DateRange;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TrieDateField;

/** {@link SelectStatement} Selects containing a Solr query expressed in the CQL WHERE clause */
public class NativeCQLSolrSelectStatement extends SolrSelectStatement {
  public static final String DEACTIVATION_KEY = "search-disable-solr-native-cql-query";
  // List of operators that make sense for tokenized text.
  public static final List<Operator> TOKENIZED_TEXT_ALLOWED_OPERATORS =
      ImmutableList.of(
          Operator.LIKE,
          Operator.LIKE_CONTAINS,
          Operator.LIKE_MATCHES,
          Operator.LIKE_PREFIX,
          Operator.LIKE_SUFFIX,
          Operator.IS_NOT);
  // Operators likely to produce large filters worth putting into the filter cache
  public static final List<Operator> SOLR_FQ_OPERATORS =
      ImmutableList.of(
          Operator.IS_NOT, Operator.NEQ, Operator.GT, Operator.GTE, Operator.LT, Operator.LTE);

  private static final Joiner COMMA_JOINER = Joiner.on(", ");
  private static final Joiner OR_JOINER = Joiner.on(" OR ");
  private static final Joiner AND_JOINER = Joiner.on(" AND ");
  private static final boolean DEFAULT_TERMINATE_EARLY = false;
  private final SecondaryIndexManager secondaryIndexManager;
  private final boolean isTermaniteEarly;

  public NativeCQLSolrSelectStatement(
      CQLStatement statement,
      QueryOptions options,
      QueryState state,
      Map<String, ByteBuffer> customPayload) {
    super(statement);
    SolrQueryType type = SolrQueryType.fromStatement(statement, options, state);
    Preconditions.checkArgument(
        type == SolrQueryType.NATIVE_CQL,
        "The provided statement is not a valid native CQL search statement.");

    ColumnFamilyStore cfs =
        SchemaManager.instance
            .getKeyspaceInstance(getKeyspace())
            .getColumnFamilyStore(getColumnFamily());
    this.secondaryIndexManager = cfs.indexManager;
    if (customPayload != null) {
      ByteBuffer terminateEarlyBB =
          customPayload.get(CommonParams.DOCID_SORTED_SEGMENT_TERMINATE_EARLY);
      try {
        isTermaniteEarly =
            terminateEarlyBB == null
                ? DEFAULT_TERMINATE_EARLY
                : Boolean.parseBoolean(ByteBufferUtil.string(terminateEarlyBB));
      } catch (CharacterCodingException e) {
        throw new InvalidRequestException(
            "The provided statement is invalid " + statement.toString(), e);
      }
    } else {
      isTermaniteEarly = DEFAULT_TERMINATE_EARLY;
    }
  }

  @Override
  public CqlSolrQueryRequest toSolrQueryRequest(
      SolrCore core, QueryOptions options, QueryState state) {
    validate(options);

    SearchStatementRestrictions dseRestrictions =
        (SearchStatementRestrictions) statement.getRestrictions();

    List<Expression> expressions = new LinkedList<>();

    if (!dseRestrictions.isPartitionKeyRoutable()) {
      // Partition key routing and filtering are already going to be handled by
      // CqlSolrQueryExecutor.
      Restrictions partitionKeyRestrictions =
          dseRestrictions.getRestrictions(ColumnMetadata.Kind.PARTITION_KEY);
      expressions.addAll(restrictionsToExpressions(partitionKeyRestrictions, options));
    }
    Restrictions clusteringKeyRestrictions =
        dseRestrictions.getRestrictions(ColumnMetadata.Kind.CLUSTERING);
    expressions.addAll(restrictionsToExpressions(clusteringKeyRestrictions, options));
    List<Expression> nonKeyExpressions =
        this.statement.getRowFilter(state, options).getExpressions();
    expressions.addAll(nonKeyExpressions);
    expressions.addAll(
        restrictionsToExpressions(dseRestrictions.getIsNotNullRestrictions(), options));

    RequestBuilder builder = new RequestBuilder(core, options);

    for (Expression current : expressions) {
      AbstractType colType = current.column().type.asCQL3Type().getType();
      if (current.kind() != Expression.Kind.SIMPLE
          && current.kind() != Expression.Kind.MAP_EQUALITY) {
        throw new InvalidRequestException(
            "Expression not supported: " + current.getClass().getName() + ": " + current);
      }
      builder.addExpression(
          current.column().name.toString(), current.operator(), current.getIndexValue(), colType);
    }

    return builder.build();
  }

  private List<Expression> restrictionsToExpressions(
      Restrictions restrictions, QueryOptions queryOptions) {
    List<Expression> expressions = new ArrayList<>(restrictions.size());

    for (ColumnMetadata currColDef : restrictions.getColumnDefs()) {
      expressions.addAll(
          restrictionsToExpressions(restrictions.getRestrictions(currColDef), queryOptions));
    }

    return expressions;
  }

  private List<Expression> restrictionsToExpressions(
      Set<Restriction> restrictions, QueryOptions queryOptions) {
    RowFilter rowFilter = RowFilter.create(restrictions.size());

    for (Restriction currRestriction : restrictions) {
      if (!(currRestriction instanceof TokenRestriction)) {
        currRestriction.addToRowFilter(rowFilter, secondaryIndexManager, queryOptions);
      }
    }

    return rowFilter.getExpressions();
  }

  private final class RequestBuilder {
    private static final String SOLR_STD_QPARSER_CLAUSE_TEMPLATE =
        "{!v='%s:(%s)'}"; // Search terms are () enclosed. See DSP-13767
    private static final String SOLR_TUPLE_QPARSER_CLAUSE_TEMPLATE =
        "{!tuple v='%s:(%s)'}"; // Search terms are () enclosed. See DSP-13767
    private final List<String> solrQClauses;
    private final List<String> solrFQClauses;
    private final SolrCore core;
    private final QueryOptions options;

    // colName -> (templateForSolrClause, leftValue, rightValue)
    private final Map<String, Triple<String, String, String>> ranges;

    private final CassandraSolrTypeMapper typeMapper;

    RequestBuilder(SolrCore core, QueryOptions options) {
      this.core = core;
      this.options = options;
      this.solrQClauses = new LinkedList<>();
      this.solrFQClauses = new LinkedList<>();
      this.ranges = new HashMap<>();
      this.typeMapper = ((CassandraSolrConfig) core.getSolrConfig()).getTypeMapper();
    }

    public void addExpression(
        String columnName, Operator operator, ByteBuffer bbValue, AbstractType type)
        throws InvalidRequestException {
      type = type.asCQL3Type().getType();
      if (SchemaTool.isTupleOrTupleCollection(type)) {
        if (type.isCollection()) {
          throw new InvalidRequestException("Searching collections of tuples is unsupported");
        } else {
          TupleType tupleType = (TupleType) type;
          ByteBuffer[] values = tupleType.split(bbValue);
          for (int i = 0; i < tupleType.size(); i++) {
            ByteBuffer currentValue =
                i >= values.length || values[i] == null
                    ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                    : values[i];
            String deepestColumnName =
                tupleType.isUDT()
                    ? ((UserType) tupleType).fieldName(i).toString()
                    : "field" + (i + 1);
            String tupleSubFieldColumnName = columnName + "." + deepestColumnName;
            if (SchemaTool.isTupleOrTupleCollection(tupleType.type(i))) {
              throw new InvalidRequestException(
                  "Searching tuples with nested tuples is unsupported");
            } else if (tupleType.type(i).isCollection()) {
              throw new InvalidRequestException(
                  "Searching tuples with nested collections is unsupported");
            } else {
              addExpression(
                  tupleSubFieldColumnName,
                  operator,
                  currentValue,
                  tupleType.type(i).asCQL3Type().getType());
            }
          }
        }
      } else {
        Pair<String, String> solrKeyValue =
            getSolrKeyValuePair(columnName, operator, bbValue, type);
        String newRequestClause =
            buildNewSearchClause(solrKeyValue.getKey(), operator, solrKeyValue.getValue());
        if (newRequestClause != null) {
          if (SOLR_FQ_OPERATORS.contains(operator)) {
            solrFQClauses.add(newRequestClause);
          } else {
            solrQClauses.add(newRequestClause);
          }
        }
      }
    }

    /*
     * Str/TextFiled token etc notes:
     *  - LIKE MATCHES
     *   - Str: As special chars are escaped, be it double quoted or not we get an exact match behavior.
     *   - Text: Solr tokenized query. if it comes enclosed in double quotes we need to not escape them. If it comes without all is normal. 2 possible behaviors.
     *  - LIKE WILDCARD
     *   - Str: As expected, nothing special
     *   - Text: Solr doesn't tokenize query for wildcards. it will fail validation if not double quoted. Always should reach here with double quotes.
     */
    private String buildNewSearchClause(String columnName, Operator operator, String solrValue) {
      String res = null;
      String clauseTemplate =
          columnName.contains(".")
              ? SOLR_TUPLE_QPARSER_CLAUSE_TEMPLATE
              : SOLR_STD_QPARSER_CLAUSE_TEMPLATE;
      switch (operator) {
        case CONTAINS:
        case EQ:
        case LIKE_MATCHES:
          // For the record:
          //    For StrField
          //      field:"" matches empty value
          //      -field:["" TO *] matches missing values fields. Empty strings are seen as missing
          // as well
          //      -field:[* TO *] matches real value missing fields
          //    For Tokenized text
          //      -field:["" TO *] and -field:[* TO *] match the same
          //      field:"" is not possible. We'd need a C* IS NULL OR EMPTY
          res = String.format(clauseTemplate, columnName, "\"" + solrValue + "\"");
          break;
        case LT:
          ranges.put(
              columnName,
              Triple.of(
                  clauseTemplate,
                  ranges.get(columnName) == null ? "{*" : ranges.get(columnName).getMiddle(),
                  solrValue + "}"));
          break;
        case LTE:
          ranges.put(
              columnName,
              Triple.of(
                  clauseTemplate,
                  ranges.get(columnName) == null ? "[*" : ranges.get(columnName).getMiddle(),
                  solrValue + "]"));
          break;
        case GTE:
          ranges.put(
              columnName,
              Triple.of(
                  clauseTemplate,
                  "[" + solrValue,
                  ranges.get(columnName) == null ? "*]" : ranges.get(columnName).getRight()));
          break;
        case GT:
          ranges.put(
              columnName,
              Triple.of(
                  clauseTemplate,
                  "{" + solrValue,
                  ranges.get(columnName) == null ? "*}" : ranges.get(columnName).getRight()));
          break;
        case LIKE_CONTAINS:
          res = String.format(clauseTemplate, columnName, "*" + solrValue + "*");
          break;
        case LIKE_PREFIX:
          res = String.format(clauseTemplate, columnName, solrValue + "*");
          break;
        case LIKE_SUFFIX:
          res = String.format(clauseTemplate, columnName, "*" + solrValue);
          break;
        case NEQ:
          res = "-" + String.format(clauseTemplate, columnName, "\"" + solrValue + "\"");
          break;
        case IS_NOT:
          // Only IS NOT NULL is allowed in syntax. 'field:*' returns all those with a value
          res = String.format(clauseTemplate, columnName, "*");
          break;
        case CONTAINS_KEY:
        case IN:
          res = String.format(clauseTemplate, columnName, solrValue);
          break;
        default:
          throw new InvalidRequestException(
              "Expression not supported: ["
                  + columnName
                  + "] ["
                  + operator
                  + "] ["
                  + solrValue
                  + "]");
      }

      return res;
    }

    private Pair<String, String> getSolrKeyValuePair(
        String columnName, Operator operator, ByteBuffer value, AbstractType<?> type) {
      Preconditions.checkNotNull(value);

      Pair<String, String> solrNameValuePair = null;
      switch (operator) {
        case IN:
          Value rawValue =
              Value.fromSerialized(
                  value, ListType.getInstance(type, false), options.getProtocolVersion());
          Stream<Object> strValues =
              rawValue.getElements().stream()
                  .map(
                      v ->
                          "\""
                              + getSolrKeyAndValueFromBB(columnName, operator, v, type).getValue()
                              + "\"");
          solrNameValuePair = Pair.of(columnName, OR_JOINER.join(strValues.iterator()));
          break;
        case IS_NOT:
          solrNameValuePair = Pair.of(columnName, "*");
          break;
        default:
          solrNameValuePair = getSolrKeyAndValueFromBB(columnName, operator, value, type);
          break;
      }

      return solrNameValuePair;
    }

    private Pair<String, String> getSolrKeyAndValueFromBB(
        String columnName, Operator operator, ByteBuffer value, AbstractType type) {
      Object composedObject;
      String fieldName = columnName;

      if (type instanceof MapType) {
        if (operator.equals(Operator.CONTAINS_KEY)) {
          // Returns 'dynField:*'
          AbstractType<String> keysType = ((MapType) type).getKeysType();
          fieldName = (String) safeCompose(columnName, keysType, value);
          return Pair.of(fieldName, "*");
        } else {
          // Carefull with dyn fields
          AbstractType<String> keysType = ((MapType) type).getKeysType();
          AbstractType valuesType = ((MapType) type).getValuesType();
          CompositeType keyValueCompositeType = CompositeType.getInstance(keysType, valuesType);
          ByteBuffer[] keyValuePairBB = keyValueCompositeType.split(value);

          fieldName = (String) safeCompose(columnName, keysType, keyValuePairBB[0]);
          composedObject = safeCompose(columnName, valuesType, keyValuePairBB[1]);
          type = valuesType;
        }
      } else if (operator.equals(Operator.CONTAINS)
          && (type instanceof SetType || type instanceof ListType)) {
        CollectionType colType = (CollectionType) type;
        AbstractType elementType =
            colType.kind.equals(Kind.LIST)
                ? ((ListType) colType).getElementsType()
                : ((SetType) colType).getElementsType();
        elementType = elementType.asCQL3Type().getType();

        composedObject = safeCompose(columnName, elementType, value);
        type = elementType;

      } else if (type instanceof TimeUUIDType && operator.isSlice()) {
        // Timeuuid needs to be a uuid for EQ but a date string for range queries
        long timeInMillis = ((TimeUUIDType) type).toTimeInMillis(value);
        String solrValue = Instant.ofEpochMilli(timeInMillis).toString();
        return Pair.of(fieldName, solrValue);
      } else if (type instanceof DateRangeType) {
        // Timeuuid needs to be a uuid for EQ but a date string for range queries
        DateRange range = (DateRange) safeCompose(columnName, type, value);
        String solrValue = range.formatToSolrString();
        return Pair.of(fieldName, solrValue);
      } else {
        composedObject = safeCompose(columnName, type, value);
      }

      // Prevent feeding a null downstream. Methods are not null resilient
      if (composedObject == null) {
        throw new InvalidRequestException(
            "Could not handle value for field " + columnName + " with value null");
      }

      SchemaField field = core.getLatestSchema().getFieldOrNull(fieldName);
      if (field == null) {
        throw new InvalidRequestException("Field [" + fieldName + "] doesn't exist.");
      }
      FieldType solrFieldType = field.getType();
      String cassandraStrValue = typeMapper.formatToCassandraType(composedObject, solrFieldType);
      // date C* strs may contain special chars we don't want to escape
      cassandraStrValue = maybeAlterForSolr(cassandraStrValue, type, solrFieldType);
      String solrValue = typeMapper.formatToSolrType(cassandraStrValue, solrFieldType);
      return Pair.of(fieldName, solrValue);
    }

    private String maybeAlterForSolr(String value, AbstractType cType, FieldType sType) {
      if (cType instanceof InetAddressType) {
        // Remove initial '/'
        return value.substring(1);
      } else if (sType instanceof TrieDateField || cType instanceof TimeUUIDType) {
        // Avoid escaping date special chars such as '-' or ':'. Also a timeuuid can be converted to
        // a date for slices
        return value;
      } else {
        return escapeQueryChars(value);
      }
    }

    private Object safeCompose(String columnName, AbstractType<?> type, ByteBuffer value) {
      try {
        return type.compose(value);
      } catch (NullPointerException e) {
        throw new InvalidRequestException(
            "Could not handle value for field " + columnName + " with value " + value);
      }
    }

    /* Variarion on Solr's ClientUtils.escapeQueryChars that escapes with double backslash instead of single
     * and also works for '
     *  - Using the {!v} template requires us to use double escaping
     *  - ' coming from C* needs to be escaped as well for the {!v} template
     */
    private String escapeQueryChars(String s) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        // These characters are part of the query syntax and must be escaped
        if (c == '\\') {
          sb.append("\\\\\\");
        } else if (c == '+'
            || c == '-'
            || c == '!'
            || c == '('
            || c == ')'
            || c == ':'
            || c == '^'
            || c == '['
            || c == ']'
            || c == '\"'
            || c == '{'
            || c == '}'
            || c == '~'
            || c == '*'
            || c == '?'
            || c == '|'
            || c == '&'
            || c == ';'
            || c == '/'
            || Character.isWhitespace(c)) {
          sb.append("\\\\");
        } else if (c == '\'') {
          sb.append("\\");
        }

        sb.append(c);
      }
      return sb.toString();
    }

    private CqlSolrQueryRequest build() {
      // Process collected ranges
      for (Entry<String, Triple<String, String, String>> current : ranges.entrySet()) {
        String template = current.getValue().getLeft();
        String left = current.getValue().getMiddle();
        String right = current.getValue().getRight();
        solrFQClauses.add(String.format(template, current.getKey(), left + " TO " + right));
      }

      ModifiableSolrParams params = new ModifiableSolrParams();
      // ORDER BY to Solr 'sort'
      if (!statement.parameters.orderings.isEmpty()) {
        List<String> sortClauses = new ArrayList<>(statement.parameters.orderings.size());
        for (Entry<ColumnMetadata.Raw, Boolean> current :
            statement.parameters.orderings.entrySet()) {
          sortClauses.add(current.getKey().rawText() + (current.getValue() ? " desc" : " asc"));
        }
        params.set(CommonParams.SORT, COMMA_JOINER.join(sortClauses));
      }

      params.set(
          CommonParams.Q,
          solrQClauses.isEmpty() ? "*:*" : "(" + AND_JOINER.join(solrQClauses) + ")");

      for (String current : solrFQClauses) {
        params.add(CommonParams.FQ, current);
      }

      if (isTermaniteEarly) {
        params.set(CommonParams.DOCID_SORTED_SEGMENT_TERMINATE_EARLY, isTermaniteEarly);
      }

      return new CqlSolrQueryRequest(core, params);
    }
  }
}
