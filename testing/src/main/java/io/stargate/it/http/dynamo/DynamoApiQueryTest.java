package io.stargate.it.http.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.ApiServiceExtension;
import io.stargate.it.http.ApiServiceSpec;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
public class DynamoApiQueryTest extends BaseDynamoApiTest {
  private String tableName = "normal_table";
  private String tableName2 = "table_with_reserved_keyword";
  private Table proxyTable, awsTable;
  private Table proxyTable2, awsTable2;

  @BeforeEach
  public void setUpTable() {
    createTable();
    proxyTable = new DynamoDB(proxyClient).getTable(tableName);
    awsTable = new DynamoDB(awsClient).getTable(tableName);
    for (Item item : getItems()) {
      proxyTable.putItem(item);
      awsTable.putItem(item);
    }

    proxyTable2 = new DynamoDB(proxyClient).getTable(tableName2);
    awsTable2 = new DynamoDB(awsClient).getTable(tableName2);
    for (Item item : getItemsWithReservedWord()) {
      proxyTable2.putItem(item);
      awsTable2.putItem(item);
    }
  }

  @AfterEach
  public void deleteTable() {
    awsClient.deleteTable(tableName);
    proxyClient.deleteTable(tableName);
    awsClient.deleteTable(tableName2);
    proxyClient.deleteTable(tableName2);
  }

  /**
   * Test keyConditionExpression with the following format:<br>
   * partitionKeyName = :partitionkeyval
   */
  @Test
  public void testBasicPartitionKeyExpressionQuery() {
    // match a single result
    QuerySpec query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name")
            .withValueMap(new ValueMap().withString(":name", "bob"));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));

    // match multiple results
    query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :v_name")
            .withValueMap(new ValueMap().withString(":v_name", "alice"));
    Set<Item> results = collectResults(proxyTable.query(query));
    assertEquals(3, results.size());
    assertTrue(getItems().containsAll(results));
    assertEquals(collectResults(awsTable.query(query)), results);
  }

  /**
   * Test KeyConditionExpression with the following format:<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName OP :sortkeyval<br>
   * where OP can be "=", ">", "<", ">=", or "<="
   */
  @Test
  public void testSortKeyOperatorComparisonExpressionQuery() {
    // larger than operator, one result
    QuerySpec query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name and Birthday > :bday")
            .withValueMap(new ValueMap().withString(":name", "bob").withNumber(":bday", 19991231));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    // smaller than operator, two results
    query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :v_name AND Birthday < :v_day")
            .withValueMap(
                new ValueMap().withString(":v_name", "alice").withNumber(":v_day", 20000102));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(2, collectResults(proxyTable.query(query)).size());

    // larger than or equal to operator, three results
    query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name AND Birthday >= :bday")
            .withValueMap(
                new ValueMap().withString(":name", "alice").withNumber(":bday", 19801231));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(3, collectResults(proxyTable.query(query)).size());

    // smaller than or equal to operator, one result
    query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name AND Birthday <= :bday")
            .withValueMap(new ValueMap().withString(":name", "bob").withNumber(":bday", 20000101));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    // equivalence operator, one result
    query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name AND Birthday = :bday")
            .withValueMap(new ValueMap().withString(":name", "bob").withNumber(":bday", 20000101));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());
  }

  /**
   * Test KeyConditionExpression with the following format:<br>
   * partitionKeyName = :partitionkeyval AND sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2
   */
  @Test
  public void testSortKeyBetweenComparisonExpressionQuery() {
    QuerySpec query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name AND Birthday between :lower AND :upper")
            .withValueMap(
                new ValueMap()
                    .withString(":name", "alice")
                    .withNumber(":lower", 20000101)
                    .withNumber(":upper", 20000101));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("Username = :name AND Birthday BETWEEN :lower AND :upper")
            .withValueMap(
                new ValueMap()
                    .withString(":name", "alice")
                    .withNumber(":lower", 20000101)
                    .withNumber(":upper", 20010101));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(2, collectResults(proxyTable.query(query)).size());
  }

  /**
   * Test the case where a key is a reserved keyword in DynamoDB, in which case user shall use a
   * placeholder (starting with a hashtag #) to represent the name
   */
  @Test
  public void testReservedKeywordExpressionQuery() {
    QuerySpec query =
        new QuerySpec()
            .withKeyConditionExpression("#N = :name AND #D BETWEEN :lower and :upper")
            .withNameMap(new NameMap().with("#N", "Name").with("#D", "Data"))
            .withValueMap(
                new ValueMap()
                    .withString(":name", "alice")
                    .withString(":lower", "San Antonio")
                    .withString(":upper", "San Jose"));
    assertEquals(collectResults(awsTable2.query(query)), collectResults(proxyTable2.query(query)));
    assertEquals(2, collectResults(proxyTable2.query(query)).size());

    query
        .withKeyConditionExpression("#N = :name AND #D <= :upper")
        .withValueMap(new ValueMap().withString(":name", "alice").withString(":upper", "San Jose"));
    assertEquals(collectResults(awsTable2.query(query)), collectResults(proxyTable2.query(query)));

    query
        .withKeyConditionExpression("#N = :name AnD #D > :lower")
        .withValueMap(
            new ValueMap().withString(":name", "alice").withString(":lower", "San Antonio"));
    assertEquals(collectResults(awsTable2.query(query)), collectResults(proxyTable2.query(query)));
  }

  @Test
  public void testFilterExpression() {
    QuerySpec query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D>:number")
            .withNameMap(new NameMap().with("#N", "Username").with("#D", "Deposit"))
            .withValueMap(new ValueMap().withString(":name", "alice").withNumber(":number", 100));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D>=:number")
            .withNameMap(new NameMap().with("#N", "Username").with("#D", "Deposit"))
            .withValueMap(new ValueMap().withString(":name", "alice").withNumber(":number", 100));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(2, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D.#OUTER = :number")
            .withNameMap(
                new NameMap().with("#N", "Username").with("#D", "dict").with("#OUTER", "outer"))
            .withValueMap(new ValueMap().withString(":name", "Charlie").withNumber(":number", 2));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(0, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D.#OUTER <> :number")
            .withNameMap(
                new NameMap().with("#N", "Username").with("#D", "dict").with("#OUTER", "outer"))
            .withValueMap(new ValueMap().withString(":name", "Charlie").withNumber(":number", 2));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D.#OUTER.lst[0].#INNER.nested[0][0][1] = :number")
            .withNameMap(
                new NameMap()
                    .with("#N", "Username")
                    .with("#D", "dict")
                    .with("#OUTER", "outer")
                    .with("#INNER", "inner"))
            .withValueMap(new ValueMap().withString(":name", "Charlie").withNumber(":number", 2));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D.#OUTER.lst[0].#INNER.nested[0][0][1] = :number")
            .withNameMap(
                new NameMap()
                    .with("#N", "Username")
                    .with("#D", "dict")
                    .with("#OUTER", "outer")
                    .with("#INNER", "inner"))
            .withValueMap(new ValueMap().withString(":name", "Charlie").withNumber(":number", 1));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(0, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N=:name")
            .withFilterExpression("#D.#OUTER.lst[0].#INNER.nested[0][0] = :numberList")
            .withNameMap(
                new NameMap()
                    .with("#N", "Username")
                    .with("#D", "dict")
                    .with("#OUTER", "outer")
                    .with("#INNER", "inner"))
            .withValueMap(
                new ValueMap()
                    .withString(":name", "Charlie")
                    .withList(":numberList", Arrays.asList(1, 2, 3)));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N = :name")
            .withFilterExpression("Deposit>:number AND NOT Loan >= :loan")
            .withNameMap(new NameMap().with("#N", "Username"))
            .withValueMap(
                new ValueMap()
                    .withString(":name", "alice")
                    .withNumber(":number", 100)
                    .withNumber(":loan", 199.9999));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N = :name")
            .withFilterExpression("Deposit in (:n1)")
            .withNameMap(new NameMap().with("#N", "Username"))
            .withValueMap(new ValueMap().withString(":name", "alice").withNumber(":n1", 100));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(1, collectResults(proxyTable.query(query)).size());

    query =
        new QuerySpec()
            .withKeyConditionExpression("#N = :name")
            .withFilterExpression("Deposit in (:n1, :n2, :n3)")
            .withNameMap(new NameMap().with("#N", "Username"))
            .withValueMap(
                new ValueMap()
                    .withString(":name", "alice")
                    .withNumber(":n1", 100)
                    .withNumber(":n2", 2000)
                    .withNumber(":n3", 3000));
    assertEquals(collectResults(awsTable.query(query)), collectResults(proxyTable.query(query)));
    assertEquals(2, collectResults(proxyTable.query(query)).size());
  }

  private Set<Item> collectResults(ItemCollection<QueryOutcome> outcomes) {
    return StreamSupport.stream(outcomes.spliterator(), false).collect(Collectors.toSet());
  }

  private Set<Item> getItems() {
    Set<Item> items = new HashSet<>();
    items.add(
        new Item()
            .withPrimaryKey("Username", "alice", "Birthday", 20000101)
            .withString("Sex", "F")
            .withNumber("Deposit", 100)
            .withNumber("Loan", 200));
    items.add(
        new Item()
            .withPrimaryKey("Username", "alice", "Birthday", 19801231)
            .withString("Sex", "F")
            .withNumber("Deposit", 2000));
    items.add(
        new Item()
            .withPrimaryKey("Username", "alice", "Birthday", 20000304)
            .withString("Sex", "F"));
    items.add(
        new Item().withPrimaryKey("Username", "bob", "Birthday", 20000101).withString("Sex", "M"));

    Map<String, Object> dict = new HashMap<>();
    Map<String, Object> dict2 = new HashMap<>();
    dict.put("outer", dict2);
    List<Object> list = new ArrayList<>();
    Map<String, Object> dict3 = new HashMap<>();
    list.add(dict3);
    dict2.put("lst", list);
    Map<String, Object> dict4 = new HashMap<>();
    dict3.put("inner", dict4);
    dict4.put("nested", Arrays.asList(Arrays.asList(Arrays.asList(1, 2, 3))));

    items.add(
        new Item()
            .withPrimaryKey("Username", "Charlie", "Birthday", 20100801)
            .withMap("dict", dict));
    return items;
  }

  private Set<Item> getItemsWithReservedWord() {
    // "Name" is a reserved keyword
    Set<Item> items = new HashSet<>();
    items.add(new Item().withPrimaryKey("Name", "alice", "Data", "San Diego"));
    items.add(new Item().withPrimaryKey("Name", "alice", "Data", "San Francisco"));
    items.add(new Item().withPrimaryKey("Name", "alice", "Data", "New York"));
    items.add(new Item().withPrimaryKey("Name", "bob", "Data", "New York"));
    return items;
  }

  private void createTable() {
    CreateTableRequest req =
        new CreateTableRequest()
            .withTableName(tableName)
            .withProvisionedThroughput(
                new ProvisionedThroughput()
                    .withReadCapacityUnits(100L)
                    .withWriteCapacityUnits(100L))
            .withKeySchema(
                new KeySchemaElement("Username", KeyType.HASH),
                new KeySchemaElement("Birthday", KeyType.RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition("Username", ScalarAttributeType.S),
                new AttributeDefinition("Birthday", ScalarAttributeType.N));
    proxyClient.createTable(req);
    awsClient.createTable(req);

    CreateTableRequest req2 =
        new CreateTableRequest()
            .withTableName(tableName2)
            .withProvisionedThroughput(
                new ProvisionedThroughput()
                    .withReadCapacityUnits(100L)
                    .withWriteCapacityUnits(100L))
            .withKeySchema(
                new KeySchemaElement("Name", KeyType.HASH),
                new KeySchemaElement("Data", KeyType.RANGE))
            .withAttributeDefinitions(
                new AttributeDefinition("Name", ScalarAttributeType.S),
                new AttributeDefinition("Data", ScalarAttributeType.S));
    proxyClient.createTable(req2);
    awsClient.createTable(req2);
  }
}
