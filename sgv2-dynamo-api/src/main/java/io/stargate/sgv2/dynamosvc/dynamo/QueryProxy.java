package io.stargate.sgv2.dynamosvc.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.Schema;
import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryProxy extends Proxy {

  private static final Logger logger = LoggerFactory.getLogger(QueryProxy.class);

  private static final Map<String, Predicate> operatorConverter =
      new HashMap<String, Predicate>() {
        {
          put("=", Predicate.EQ);
          put("<=", Predicate.LTE);
          put("<", Predicate.LT);
          put(">=", Predicate.GTE);
          put(">", Predicate.GT);
        }
      };

  /**
   * Basic partition key equality pattern
   *
   * <p>Example:<br>
   * partitionKeyName = :partitionkeyval
   */
  private static final Pattern EQUALITY_PATTERN =
      Pattern.compile("^\\s*(\\S+)\\s*=\\s*(:\\S+)\\s*$");

  /**
   * Basic partition key equality pattern + Sort key operation comparison pattern
   *
   * <p>Examples:<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName < :sortkeyval<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName <= :sortkeyval<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName > :sortkeyval<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName >= :sortkeyval<br>
   */
  private static final Pattern OP_COMPARISON_PATTERN =
      Pattern.compile("(?i)(\\S+)\\s*=\\s*(:\\S+)\\s*AND\\s*(\\S+)\\s*([<>=]+)\\s*(:\\S+)");

  /**
   * Basic partition key equality pattern + Sort key interval comparison pattern
   *
   * <p>Example:<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2
   */
  private static final Pattern BETWEEN_PATTERN =
      Pattern.compile(
          "(?i)(\\S+)\\s*=\\s*(:\\S+)\\s*AND\\s*(\\S+)\\s*BETWEEN\\s*(:\\S+)\\s*AND\\s*(:\\S+)");

  /**
   * Basic partition key equality pattern + Sort key begins_with pattern (cannot use with number)
   *
   * <p>Example:<br>
   * partitionKeyName = :partitionkeyval AND begins_with ( sortKeyName, :sortkeyval )
   */
  private static final Pattern BEGIN_WITH_PATTERN =
      Pattern.compile(
          "(?i)(\\S+)\\s*=\\s*(:\\S+)\\s*AND\\s*begins_with\\s*\\(\\s*(\\S+),\\s*(:\\S+)\\s*\\)");

  public QueryResult query(QueryRequest queryRequest, StargateBridgeClient bridge) {
    // Get table schema
    final String tableName = queryRequest.getTableName();
    Schema.CqlTable table =
        bridge
            .getTable(KEYSPACE_NAME, tableName)
            .orElseThrow(() -> new IllegalArgumentException("Table not found: " + tableName));
    QueryOuterClass.ColumnSpec partitionKey = table.getPartitionKeyColumns(0);
    List<QueryOuterClass.ColumnSpec> clusteringKeys = table.getClusteringKeyColumnsList();
    Optional<QueryOuterClass.ColumnSpec> clusteringKey = Optional.empty();
    if (CollectionUtils.isNotEmpty(clusteringKeys)) {
      assert clusteringKeys.size() == 1;
      clusteringKey = Optional.of(clusteringKeys.get(0));
    }
    // TODO: support legacy KeyConditions parameter
    if (StringUtils.isEmpty(queryRequest.getKeyConditionExpression())) {
      throw new IllegalArgumentException("Must provide a key condition expression");
    }
    QueryOuterClass.Query query =
        constructByKeyConditionExpression(queryRequest, partitionKey, clusteringKey);

    // TODO: support FilterExpression (https://github.com/li-boxuan/stargate/issues/16)

    // TODO: support ProjectionExpression which retrieves a subset of row
    QueryOuterClass.Response response = bridge.executeQuery(query);
    List<Map<String, Object>> resultRows = convertRows(response.getResultSet());
    QueryResult result = new QueryResult();
    if (!resultRows.isEmpty()) {
      result.withItems(
          resultRows.stream()
              .map(
                  r ->
                      r.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  Map.Entry::getKey,
                                  entry -> DataMapper.toDynamo(entry.getValue()))))
              .collect(Collectors.toList()));
    }
    return result;
  }

  /**
   * Construct the query from KeyConditionExpression.
   *
   * <p>A KeyConditionExpression must have an equality check for partition key, and optionally a
   * comparison check for clustering key (a.k.a. sort key). We use different regular expressions to
   * capture all cases.
   *
   * <p>See more at
   * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
   *
   * @param queryRequest
   * @param partitionKey
   * @param clusteringKey
   * @return
   */
  private QueryOuterClass.Query constructByKeyConditionExpression(
      QueryRequest queryRequest,
      QueryOuterClass.ColumnSpec partitionKey,
      Optional<QueryOuterClass.ColumnSpec> clusteringKey) {
    final String keyConditionExpression = queryRequest.getKeyConditionExpression();
    Map<String, String> expressionAttributeNames = queryRequest.getExpressionAttributeNames();
    Map<String, AttributeValue> expressionAttributeValues =
        queryRequest.getExpressionAttributeValues();

    Matcher equalityMatcher = EQUALITY_PATTERN.matcher(keyConditionExpression);
    if (equalityMatcher.find()) {
      assert equalityMatcher.groupCount() == 2;
      AttributeValue partitionKeyValue =
          retrievePartitionKeyValue(
              equalityMatcher.group(1),
              partitionKey,
              equalityMatcher.group(2),
              expressionAttributeNames,
              expressionAttributeValues);
      return new QueryBuilder()
          .select()
          .from(KEYSPACE_NAME, queryRequest.getTableName())
          .where(partitionKey.getName(), Predicate.EQ, DataMapper.fromDynamo(partitionKeyValue))
          .build();
    }

    Matcher opComparisonMatcher = OP_COMPARISON_PATTERN.matcher(keyConditionExpression);
    if (opComparisonMatcher.find()) {
      assert opComparisonMatcher.groupCount() == 5;
      AttributeValue partitionKeyValue =
          retrievePartitionKeyValue(
              opComparisonMatcher.group(1),
              partitionKey,
              opComparisonMatcher.group(2),
              expressionAttributeNames,
              expressionAttributeValues);

      String sortKeyName = getKeyName(opComparisonMatcher.group(3), expressionAttributeNames);
      String sortKeyCompOp = opComparisonMatcher.group(4);
      Predicate predicate = operatorConverter.get(sortKeyCompOp);
      if (predicate == null) {
        throw new IllegalArgumentException(sortKeyCompOp + " is not a supported operator");
      }
      checkSortKey(sortKeyName, clusteringKey);
      AttributeValue sortKeyValue =
          getExpressionAttributeValue(expressionAttributeValues, opComparisonMatcher.group(5));
      return new QueryBuilder()
          .select()
          .from(KEYSPACE_NAME, queryRequest.getTableName())
          .where(partitionKey.getName(), Predicate.EQ, DataMapper.fromDynamo(partitionKeyValue))
          .where(sortKeyName, predicate, DataMapper.fromDynamo(sortKeyValue))
          .build();
    }

    Matcher betweenMatcher = BETWEEN_PATTERN.matcher(keyConditionExpression);
    if (betweenMatcher.find()) {
      assert betweenMatcher.groupCount() == 5;
      AttributeValue partitionKeyValue =
          retrievePartitionKeyValue(
              betweenMatcher.group(1),
              partitionKey,
              betweenMatcher.group(2),
              expressionAttributeNames,
              expressionAttributeValues);

      String sortKeyName = getKeyName(betweenMatcher.group(3), expressionAttributeNames);
      checkSortKey(sortKeyName, clusteringKey);
      AttributeValue lowerBoundValue =
          getExpressionAttributeValue(expressionAttributeValues, betweenMatcher.group(4));
      AttributeValue higherBoundValue =
          getExpressionAttributeValue(expressionAttributeValues, betweenMatcher.group(5));
      return new QueryBuilder()
          .select()
          .from(KEYSPACE_NAME, queryRequest.getTableName())
          .where(partitionKey.getName(), Predicate.EQ, DataMapper.fromDynamo(partitionKeyValue))
          .where(sortKeyName, Predicate.GTE, DataMapper.fromDynamo(lowerBoundValue))
          .where(sortKeyName, Predicate.LTE, DataMapper.fromDynamo(higherBoundValue))
          .build();
    }

    Matcher beginWithMatcher = BEGIN_WITH_PATTERN.matcher(keyConditionExpression);
    if (beginWithMatcher.find()) {
      // TODO: address this
      throw new IllegalArgumentException("Cassandra does not support begin_with query");
    }

    throw new IllegalArgumentException(keyConditionExpression + " is not a valid expression");
  }

  /**
   * Given a raw key name in expression, retrieve its real key name
   *
   * <p>A raw key name might be a key name or a key placeholder. A placeholder starts with a '#'
   * pound sign, whose actual value needs to be retrieved from attributeNames.
   *
   * <p>See more at
   * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
   *
   * @param rawKeyName
   * @param attributeNames
   * @return
   */
  private String getKeyName(String rawKeyName, Map<String, String> attributeNames) {
    if (rawKeyName.charAt(0) == '#') {
      return attributeNames.get(rawKeyName);
    } else {
      return rawKeyName;
    }
  }

  private AttributeValue retrievePartitionKeyValue(
      String rawKeyName,
      QueryOuterClass.ColumnSpec partitionKey,
      String partitionKeyValueMarker,
      Map<String, String> expressionAttributeNames,
      Map<String, AttributeValue> expressionAttributeValues) {
    String partitionKeyName = getKeyName(rawKeyName, expressionAttributeNames);
    checkPartitionKey(partitionKeyName, partitionKey);
    return getExpressionAttributeValue(expressionAttributeValues, partitionKeyValueMarker);
  }

  private void checkPartitionKey(String partitionKeyName, QueryOuterClass.ColumnSpec partitionKey) {
    if (!Objects.equals(partitionKeyName, partitionKey.getName())) {
      throw new IllegalArgumentException(
          partitionKeyName + " does not match partition key name: " + partitionKey.getName());
    }
  }

  private void checkSortKey(String sortKeyName, Optional<QueryOuterClass.ColumnSpec> sortKey) {
    if (!sortKey.isPresent()) {
      throw new IllegalArgumentException("Sort key does not exist");
    }
    if (!Objects.equals(sortKeyName, sortKey.get().getName())) {
      throw new IllegalArgumentException(
          sortKeyName + " does not match sort key name: " + sortKey.get().getName());
    }
  }
}
