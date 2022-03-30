package io.stargate.sgv2.dynamosvc.dynamo;

import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import java.io.IOException;

public class ItemProxy {
  public PutItemResult putItem(PutItemRequest putItemRequest, StargateBridgeClient bridge)
      throws IOException {
    // Step 1: Fetch (cached) table schema

    // Step 2: Alter table column definition if needed

    // Step 3: Write data

    // Step 4 (TODO): Write index if applicable
    return new PutItemResult();
  }
}
