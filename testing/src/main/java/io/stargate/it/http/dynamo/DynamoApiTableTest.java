package io.stargate.it.http.dynamo;

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
  public void testCreateTable() {
    // TODO: retrieve table again and compare it with DynamoDB
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
  }
}
