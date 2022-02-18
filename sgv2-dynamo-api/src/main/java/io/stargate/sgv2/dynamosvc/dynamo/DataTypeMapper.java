package io.stargate.sgv2.dynamosvc.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;

public class DataTypeMapper {
  public static String fromDynamo(DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoType) {
    switch (dynamoType) {
      case S:
        return "text";
      case N:
        return "double";
      case B:
        return "blob";
      case BOOL:
        return "boolean";
      case M:
        return "map";
      case L:
        return "list";
        // TODO: revisit the following
      case NULL:
        return null;
      case SS:
      case NS:
      case BS:
        return "set";
      default:
        throw new IllegalArgumentException("Dynamo Type " + dynamoType + " not supported");
    }
  }
}
