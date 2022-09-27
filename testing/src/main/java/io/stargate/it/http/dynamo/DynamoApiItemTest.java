package io.stargate.it.http.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.ApiServiceExtension;
import io.stargate.it.http.ApiServiceSpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
public class DynamoApiItemTest extends BaseDynamoApiTest {

  private String tableName = "item_api_test_table";

  @BeforeEach
  public void setUpTable() {
    createTable();
  }

  @AfterEach
  public void deleteTable() {
    awsClient.deleteTable(tableName);
    proxyClient.deleteTable(tableName);
  }

  @Test
  public void testBasicCreateAndGetItem() {
    DynamoDB proxyDynamoDB = new DynamoDB(proxyClient);
    Table proxyTable = proxyDynamoDB.getTable(tableName);

    // put a simple item first
    proxyTable.putItem(
        new Item()
            .withPrimaryKey("Name", "simpleName")
            .withNumber("Serial", 23)
            .withNumber("Price", 10.0));

    // put another simple item with no new column
    proxyTable.putItem(
        new Item()
            .withPrimaryKey("Name", "simpleName2")
            .withNumber("Serial", 20)
            .withNumber("Price", 0.0)
            .withString("Desc", "dummy text"));

    Map<String, Object> dict = new HashMap<>();
    dict.put("integerList", Arrays.asList(0, 1, 2));
    dict.put("stringList", Arrays.asList("aa", "bb"));
    // TODO: support null value in Stargate
    //    dict.put("nullKey", null);
    dict.put("hashMap", new HashMap<>());
    dict.put("doubleSet", new HashSet<>(Arrays.asList(1.0, 2.0)));
    Item item =
        new Item()
            .withPrimaryKey("Name", "testName")
            .withNumber("Serial", 123.0)
            .withString("ISBN", "121-1111111111")
            .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author21", "Author 22")))
            .withNumber("Price", 20.1)
            .withString("Dimensions", "8.5x11.0x.75")
            .withNumber("PageCount", 500)
            .withBoolean("InPublication", true)
            .withString("ProductCategory", "Book")
            .withMap("Dict", dict);
    proxyTable.putItem(item);
    Item proxyResult = proxyTable.getItem("Name", "testName");

    DynamoDB awsDynamoDB = new DynamoDB(awsClient);
    Table awsTable = awsDynamoDB.getTable(tableName);
    awsTable.putItem(item);
    Item awsResult = awsTable.getItem("Name", "testName");
    assertEquals(awsResult, proxyResult);
  }

  @Test
  public void testDeleteItem() {
    // test data
    Map<String, Object> dict = new HashMap<>();
    dict.put("integerList", Arrays.asList(0, 1, 2));
    dict.put("stringList", Arrays.asList("aa", "bb"));
    dict.put("hashMap", new HashMap<>());
    dict.put("doubleSet", new HashSet<>(Arrays.asList(1.0, 2.0)));
    final Item item =
        new Item()
            .withPrimaryKey("Name", "testName")
            .withNumber("Serial", 123.0)
            .withString("ISBN", "121-1111111111")
            .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author21", "Author 22")))
            .withNumber("Price", 20.1)
            .withString("Dimensions", "8.5x11.0x.75")
            .withNumber("PageCount", 500)
            .withBoolean("InPublication", true)
            .withString("ProductCategory", "Book")
            .withMap("Dict", dict);

    // DB initialize
    final DynamoDB proxyDynamoDB = new DynamoDB(proxyClient); // CDB
    final DynamoDB awsDynamoDB = new DynamoDB(awsClient); // DDB
    final Table proxyTable = proxyDynamoDB.getTable(tableName);
    final Table awsTable = awsDynamoDB.getTable(tableName);

    // put data
    proxyTable.putItem(item);
    awsTable.putItem(item);

    // delete item
    final DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("Name", "testName");
    proxyTable.deleteItem(deleteItemSpec);
    awsTable.deleteItem(deleteItemSpec);

    // compare
    final Item proxyResult = proxyTable.getItem("Name", "testName");
    final Item awsResult = awsTable.getItem("Name", "testName");

    assertEquals(awsResult, proxyResult);
  }

  @Test
  public void testDeleteItemWithCondition() {
    // test data
    Map<String, Object> dict = new HashMap<>();
    dict.put("integerList", Arrays.asList(0, 1, 2));
    dict.put("stringList", Arrays.asList("aa", "bb"));
    dict.put("hashMap", new HashMap<>());
    dict.put("doubleSet", new HashSet<>(Arrays.asList(1.0, 2.0)));
    final Item item =
        new Item()
            .withPrimaryKey("Name", "testName")
            .withNumber("Serial", 123.0)
            .withString("ISBN", "121-1111111111")
            .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author21", "Author 22")))
            .withNumber("Price", 20.1)
            .withString("Dimensions", "8.5x11.0x.75")
            .withNumber("PageCount", 500)
            .withBoolean("InPublication", true)
            .withString("ProductCategory", "Book")
            .withMap("Dict", dict);

    // DB initialize
    final DynamoDB proxyDynamoDB = new DynamoDB(proxyClient); // CDB
    final DynamoDB awsDynamoDB = new DynamoDB(awsClient); // DDB
    final Table proxyTable = proxyDynamoDB.getTable(tableName);
    final Table awsTable = awsDynamoDB.getTable(tableName);
    DeleteItemSpec deleteItemSpec;
    Item proxyResult, awsResult;

    // 1 operand EQ
    proxyTable.putItem(item);
    awsTable.putItem(item);
    deleteItemSpec =
        new DeleteItemSpec()
            .withPrimaryKey("Name", "testName")
            .withConditionExpression("#S = :vSerial")
            .withNameMap(new NameMap().with("#S", "Serial"))
            .withValueMap(new ValueMap().withNumber(":vSerial", 123));
    proxyTable.deleteItem(deleteItemSpec);
    proxyResult = proxyTable.getItem("Name", "testName");
    awsTable.deleteItem(deleteItemSpec);
    awsResult = awsTable.getItem("Name", "testName");

    assertEquals(awsResult, proxyResult);

    // 2 operands EQ (match)
    proxyTable.putItem(item);
    awsTable.putItem(item);
    deleteItemSpec =
        new DeleteItemSpec()
            .withPrimaryKey("Name", "testName")
            .withConditionExpression("#S = :vSerial AND #I = :vIsbn")
            .withNameMap(new NameMap().with("#S", "Serial").with("#I", "ISBN"))
            .withValueMap(
                new ValueMap().withNumber(":vSerial", 123).withString(":vIsbn", "121-1111111111"));
    proxyTable.deleteItem(deleteItemSpec);
    proxyResult = proxyTable.getItem("Name", "testName");
    awsTable.deleteItem(deleteItemSpec);
    awsResult = awsTable.getItem("Name", "testName");

    assertEquals(awsResult, proxyResult);

    // 2 operands EQ (either match)
    proxyTable.putItem(item);
    awsTable.putItem(item);
    deleteItemSpec =
        new DeleteItemSpec()
            .withPrimaryKey("Name", "testName")
            .withConditionExpression("#S = :vSerial OR #I = :vIsbn")
            .withNameMap(new NameMap().with("#S", "Serial").with("#I", "ISBN"))
            .withValueMap(
                new ValueMap().withNumber(":vSerial", 666).withString(":vIsbn", "121-1111111111"));
    proxyTable.deleteItem(deleteItemSpec);
    proxyResult = proxyTable.getItem("Name", "testName");
    awsTable.deleteItem(deleteItemSpec);
    awsResult = awsTable.getItem("Name", "testName");

    assertEquals(awsResult, proxyResult);

    // 2 Operands EQ (not match)
    proxyTable.putItem(item);
    awsTable.putItem(item);
    deleteItemSpec =
        new DeleteItemSpec()
            .withPrimaryKey("Name", "testName")
            .withConditionExpression("#S = :vSerial AND #I = :vIsbn")
            .withNameMap(new NameMap().with("#S", "Serial").with("#I", "ISBN"))
            .withValueMap(new ValueMap().with(":vSerial", 100).with(":vIsbn", "121-1111111111"));
    proxyTable.deleteItem(deleteItemSpec);
    proxyResult = proxyTable.getItem("Name", "testName");
    try {
      awsTable.deleteItem(deleteItemSpec); // DDB deleteItem match fail will throw exception
    } catch (ConditionalCheckFailedException e) {
      System.out.println("DynamoDB deleteItem match failed");
    }
    awsResult = awsTable.getItem("Name", "testName");

    assertEquals(awsResult, proxyResult);
  }

  private void createTable() {
    CreateTableRequest req =
        new CreateTableRequest()
            .withTableName(tableName)
            .withProvisionedThroughput(
                new ProvisionedThroughput()
                    .withReadCapacityUnits(100L)
                    .withWriteCapacityUnits(100L))
            .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));
    proxyClient.createTable(req);
    awsClient.createTable(req);
  }
}
