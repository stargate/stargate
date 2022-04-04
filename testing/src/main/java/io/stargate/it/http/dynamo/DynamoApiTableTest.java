package io.stargate.it.http.dynamo;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazonaws.services.dynamodbv2.model.*;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.ApiServiceExtension;
import io.stargate.it.http.ApiServiceSpec;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
@ExtendWith(ApiServiceExtension.class)
@ApiServiceSpec(parametersCustomizer = "buildApiServiceParameters")
public class DynamoApiTableTest extends BaseDynamoApiTest {

  @Test
  public void testDeleteNonExistentTable() {
    DeleteTableRequest req = new DeleteTableRequest().withTableName("non-existent");
    assertThrows(ResourceNotFoundException.class, () -> awsClient.deleteTable(req));
    // TODO: ensure the exceptions are the same
    assertThrows(Exception.class, () -> proxyClient.deleteTable(req));
  }

  @Test
  public void testCreateThenDeleteTable() {
    CreateTableRequest req =
        new CreateTableRequest()
            .withTableName("foo")
            .withProvisionedThroughput(
                new ProvisionedThroughput()
                    .withReadCapacityUnits(100L)
                    .withWriteCapacityUnits(100L))
            .withKeySchema(new KeySchemaElement("Name", KeyType.HASH))
            .withAttributeDefinitions(new AttributeDefinition("Name", ScalarAttributeType.S));

    awsClient.createTable(req);
    proxyClient.createTable(req);

    awsClient.deleteTable("foo");
    proxyClient.deleteTable("foo");
  }
}
