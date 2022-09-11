package io.stargate.sgv2.dynamosvc.dynamo;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.google.common.base.Preconditions;
import io.stargate.bridge.grpc.Values;
import java.io.ByteArrayOutputStream;
import java.util.Objects;

public class DataMapper {
  public static AttributeValue toDynamo(Object value) {
    AttributeValue attr = new AttributeValue();
    if (value instanceof String) {
      attr.setS((String) value);
    } else if (value instanceof Number) {
      if (Math.floor((double) value) == (double) value) {
        // if it can be converted to an integer, convert it
        // For example, in DynamoDB, if you add a value 123.0,
        // you will get 123 as response.
        attr.setN(String.valueOf(Math.round((double) value)));
      } else {
        attr.setN(value.toString());
      }
    } else if (value instanceof Boolean) {
      attr.setBOOL((Boolean) value);
    } else {
      Kryo kryo = new Kryo();
      kryo.setRegistrationRequired(false);
      Input input = new Input((byte[]) value);
      attr = kryo.readObject(input, AttributeValue.class);
    }
    return attr;
  }

  public static Object fromDynamo(AttributeValue value) {
    return fromDynamo(value, true);
  }

  public static boolean equals(AttributeValue value1, AttributeValue value2) {
    return Objects.equals(value1, value2);
  }

  public static int compareTo(AttributeValue value1, AttributeValue value2) {
    Object v1 = fromDynamo(value1, false);
    Object v2 = fromDynamo(value2, false);
    // TODO: shall we support byte array comparison
    Preconditions.checkArgument(v1 instanceof Comparable, "Value is not comparable: " + v1);
    return ((Comparable) v1).compareTo(v2);
  }

  private static byte[] serialize(AttributeValue value) {
    Kryo kryo = new Kryo();
    kryo.setRegistrationRequired(false);
    Output output = new Output(new ByteArrayOutputStream());
    kryo.writeObject(output, value);
    output.flush();
    output.close();
    return output.getBuffer();
  }

  private static Object fromDynamo(AttributeValue value, boolean convertToValues) {
    if (value.getS() != null) {
      return convertToValues ? Values.of(value.getS()) : value.getS();
    } else if (value.getN() != null) {
      return convertToValues
          ? Values.of(Double.parseDouble(value.getN()))
          : Double.parseDouble(value.getN());
    } else if (value.getBOOL() != null) {
      return convertToValues ? Values.of(value.getBOOL()) : value.getBOOL();
    } else {
      // We store everything else in binary, because they have no
      // equivalent in Cassandra.
      return convertToValues ? Values.of(serialize(value)) : serialize(value);
    }
  }
}
