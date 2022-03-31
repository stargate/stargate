package io.stargate.sgv2.dynamosvc.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class DataTypeMapper {

  public static String TEXT = "text";
  public static String DOUBLE = "double";
  public static String BOOLEAN = "boolean";
  public static String BLOB = "blob";

  public static String fromDynamo(AttributeValue value) {
    if (value.getS() != null) {
      return TEXT;
    } else if (value.getN() != null) {
      return DOUBLE;
    } else if (value.getBOOL() != null) {
      return BOOLEAN;
    } else {
      return BLOB;
    }
  }

  public static String fromDynamo(DynamoDBMapperFieldModel.DynamoDBAttributeType dynamoType) {
    switch (dynamoType) {
      case S:
        return TEXT;
      case N:
        // in DynamoDB, both integers and floating numbers are of N type
        return DOUBLE;
      case BOOL:
        return BOOLEAN;
      case NULL:
      case M:
      case L:
      case SS:
      case NS:
      case BS:
      case B:
        return BLOB;
      default:
        throw new IllegalArgumentException("Dynamo Type " + dynamoType + " not supported");
    }
  }
}
