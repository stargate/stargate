/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.producer.kafka.schema;

import io.stargate.producer.kafka.schema.codecs.BigIntegerConversion;
import io.stargate.producer.kafka.schema.codecs.BigIntegerLogicalType;
import io.stargate.producer.kafka.schema.codecs.BigIntegerLogicalType.BigIntegerTypeFactory;
import io.stargate.producer.kafka.schema.codecs.ByteConversion;
import io.stargate.producer.kafka.schema.codecs.ByteLogicalType;
import io.stargate.producer.kafka.schema.codecs.ByteLogicalType.ByteTypeFactory;
import io.stargate.producer.kafka.schema.codecs.ShortConversion;
import io.stargate.producer.kafka.schema.codecs.ShortLogicalType;
import io.stargate.producer.kafka.schema.codecs.ShortLogicalType.ShortTypeFactory;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.cassandra.stargate.schema.CQLType;
import org.apache.cassandra.stargate.schema.CQLType.Collection;
import org.apache.cassandra.stargate.schema.CQLType.Custom;
import org.apache.cassandra.stargate.schema.CQLType.MapDataType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.CQLType.Tuple;
import org.apache.cassandra.stargate.schema.CQLType.UserDefined;

public class CqlToAvroTypeConverter {
  private CqlToAvroTypeConverter() {}

  private static final Map<Native, Schema> SCHEMA_PER_NATIVE_TYPE = new HashMap<>();

  static {
    // register custom logical types
    LogicalTypes.register(ShortLogicalType.SHORT_LOGICAL_TYPE_NAME, new ShortTypeFactory());
    LogicalTypes.register(ByteLogicalType.BYTE_LOGICAL_TYPE_NAME, new ByteTypeFactory());
    LogicalTypes.register(
        BigIntegerLogicalType.BIG_INTEGER_LOGICAL_TYPE_NAME, new BigIntegerTypeFactory());

    SCHEMA_PER_NATIVE_TYPE.put(Native.ASCII, Schema.create(Type.STRING));
    SCHEMA_PER_NATIVE_TYPE.put(Native.BIGINT, Schema.create(Type.LONG));
    SCHEMA_PER_NATIVE_TYPE.put(Native.BLOB, Schema.create(Type.BYTES));
    SCHEMA_PER_NATIVE_TYPE.put(Native.BOOLEAN, Schema.create(Type.BOOLEAN));
    SCHEMA_PER_NATIVE_TYPE.put(Native.COUNTER, Schema.create(Type.LONG));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.DATE, LogicalTypes.date().addToSchema(Schema.create(Type.INT)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.DECIMAL, LogicalTypes.decimal(10).addToSchema(Schema.create(Type.BYTES)));
    SCHEMA_PER_NATIVE_TYPE.put(Native.DOUBLE, Schema.create(Type.DOUBLE));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.DURATION,
        Schema.create(Type.BYTES)); // there is no avro codec for this type, write as raw byte
    SCHEMA_PER_NATIVE_TYPE.put(Native.FLOAT, Schema.create(Type.FLOAT));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.INET,
        Schema.create(Type.BYTES)); // there is no avro codec for this type, write as raw byte
    SCHEMA_PER_NATIVE_TYPE.put(Native.INT, Schema.create(Type.INT));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.SMALLINT, new ShortLogicalType().addToSchema(Schema.create(Type.INT)));
    SCHEMA_PER_NATIVE_TYPE.put(Native.TEXT, Schema.create(Type.STRING));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.TIME, LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.TIMESTAMP, LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.TIMEUUID, LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.TINYINT, new ByteLogicalType().addToSchema(Schema.create(Type.INT)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.UUID, LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING)));
    SCHEMA_PER_NATIVE_TYPE.put(Native.VARCHAR, Schema.create(Type.STRING));
    SCHEMA_PER_NATIVE_TYPE.put(
        Native.VARINT, new BigIntegerLogicalType().addToSchema(Schema.create(Type.BYTES)));

    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
    GenericData.get().addLogicalTypeConversion(new Conversions.UUIDConversion());
    GenericData.get().addLogicalTypeConversion(new TimeConversions.DateConversion());
    GenericData.get().addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
    GenericData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    GenericData.get().addLogicalTypeConversion(new ShortConversion());
    GenericData.get().addLogicalTypeConversion(new ByteConversion());
    GenericData.get().addLogicalTypeConversion(new BigIntegerConversion());
  }

  public static Schema toAvroType(CQLType type) {
    if (type instanceof Collection) {
      return createCollectionSchema((Collection) type);
    } else if (type instanceof MapDataType) {
      return createMapSchema((MapDataType) type);
    } else if (type instanceof UserDefined) {
      return createUserDefinedSchema((UserDefined) type);
    } else if (type instanceof Tuple) {
      return creteTupleSchema((Tuple) type);
    } else if (type instanceof Custom) {
      return createCustomSchema((Custom) type);
    } else if (type instanceof Native) {
      return createNativeSchema((Native) type);
    } else {
      throw new UnsupportedOperationException(String.format("The type: %s is not supported", type));
    }
  }

  private static Schema createNativeSchema(Native type) {
    return SCHEMA_PER_NATIVE_TYPE.get(type);
  }

  private static Schema createCustomSchema(Custom type) {
    return null;
  }

  private static Schema creteTupleSchema(Tuple type) {
    return null;
  }

  private static Schema createUserDefinedSchema(UserDefined type) {
    return null;
  }

  private static Schema createMapSchema(MapDataType type) {
    // avro assumes that every key is of a string type, and convert every key to a string
    // representation automatically
    return SchemaBuilder.map().values(toAvroType(type.getValueType()));
  }

  private static Schema createCollectionSchema(Collection type) {
    return SchemaBuilder.array().items(toAvroType(type.getSubType()));
  }
}
