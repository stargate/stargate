package io.stargate.sgv2.dynamosvc;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

/**
 * This is a sample application that allows developers to quickly test DynamoDB features. Remember
 * to put your AWS access id and key as your environmental valuables.
 */
public class DynamoSampleApp {
  private static void queryIndex(DynamoDB dynamoDB) {
    Table table = dynamoDB.getTable("crud_sample_table");
    Index index = table.getIndex("Title-index");
    QuerySpec querySpec =
        new QuerySpec()
            .withKeyConditionExpression("Title = :v")
            .withValueMap(new ValueMap().withString(":v", "Book 120 Title"));
    ItemCollection<QueryOutcome> items = index.query(querySpec);
    Iterator<Item> iterator = items.iterator();
    System.out.println(iterator.hasNext());
  }

  private static void putSimpleItem(DynamoDB dynamoDB) {
    Table table = dynamoDB.getTable("crud_sample_table");
    try {

      Item item =
          new Item()
              .withPrimaryKey("Id", 120, "sid", "sid001")
              .withString("Title", "Book 120 Title")
              .withString("ISBN", "120-1111111111")
              .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author12", "Author22")))
              .withNumber("Price", 20)
              .withString("Dimensions", "8.5x11.0x.75")
              .withNumber("PageCount", 500)
              .withBoolean("InPublication", false)
              .withString("ProductCategory", "Book");
      table.putItem(item);

      item =
          new Item()
              .withPrimaryKey("Id", 121, "sid", "sid002")
              .withNumber("Title", 123)
              .withString("ISBN", "121-1111111111")
              .withStringSet("Authors", new HashSet<String>(Arrays.asList("Author21", "Author 22")))
              .withNumber("Price", 20)
              .withString("Dimensions", "8.5x11.0x.75")
              .withNumber("PageCount", 500)
              .withBoolean("InPublication", true)
              .withString("ProductCategory", "Book");
      table.putItem(item);
    } catch (Exception e) {
      System.err.println("Create items failed.");
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) {
    AmazonDynamoDB client =
        AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    ListTablesResult tables = client.listTables();
    DynamoDB dynamoDB = new DynamoDB(client);
    putSimpleItem(dynamoDB);
    queryIndex(dynamoDB);
  }
}
