package io.stargate.sgv2.dynamosvc.dynamo;

import static com.amazonaws.services.dynamodbv2.model.KeyType.HASH;
import static com.amazonaws.services.dynamodbv2.model.KeyType.RANGE;

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import io.stargate.sgv2.dynamosvc.models.PrimaryKey;
import java.util.List;

public abstract class Proxy {

  public static final String KEYSPACE_NAME = "dynamodb";
  public static final ObjectMapper awsRequestMapper =
      new ObjectMapper()
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL)
          .setPropertyNamingStrategy(
              // Map to AWS api style
              new PropertyNamingStrategy.UpperCamelCaseStrategy() {
                @Override
                public String translate(String input) {
                  String output = super.translate(input);

                  if (output != null && output.length() >= 2) {
                    switch (output) {
                      case "Ss":
                        return "SS";
                      case "Bool":
                        return "BOOL";
                      case "Ns":
                        return "NS";
                      default:
                        break;
                    }
                  }

                  return output;
                }
              });

  protected PrimaryKey getPrimaryKey(List<KeySchemaElement> keySchema) {
    PrimaryKey primaryKey = new PrimaryKey();
    for (KeySchemaElement keySchemaElement : keySchema) {
      String type = keySchemaElement.getKeyType();
      String name = keySchemaElement.getAttributeName();
      if (type.equals(HASH.toString())) {
        primaryKey.setPartitionKey(name);
      } else if (type.equals(RANGE.toString())) {
        primaryKey.setClusteringKey(name);
      }
    }
    return primaryKey;
  }
}
