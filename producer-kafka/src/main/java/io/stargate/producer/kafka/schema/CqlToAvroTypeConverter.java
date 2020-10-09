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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
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
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.cassandra.stargate.schema.CQLType;
import org.apache.cassandra.stargate.schema.CQLType.Collection;
import org.apache.cassandra.stargate.schema.CQLType.Custom;
import org.apache.cassandra.stargate.schema.CQLType.MapDataType;
import org.apache.cassandra.stargate.schema.CQLType.Native;
import org.apache.cassandra.stargate.schema.CQLType.Tuple;
import org.apache.cassandra.stargate.schema.CQLType.UserDefined;
import org.apache.commons.lang.StringUtils;

public class CqlToAvroTypeConverter {
  private static final Map<Native, Schema> SCHEMA_PER_NATIVE_TYPE = new HashMap<>();

  private CqlToAvroTypeConverter() {}

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

  /**
   * It create a Schema based on the CqlType. To see how specific types are handled, see java doc of
   * other methods.
   */
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
      return getNativeSchema((Native) type);
    } else {
      throw new UnsupportedOperationException(String.format("The type: %s is not supported", type));
    }
  }

  private static Schema getNativeSchema(Native type) {
    return SCHEMA_PER_NATIVE_TYPE.get(type);
  }

  /**
   * The custom type is saved as bytes without an attempt to deserialize it. It's the client
   * responsibility to deserialize it correctly. Currently, the class name form {@link
   * Custom#getClassName()} is not propagated in the avro message.
   */
  @VisibleForTesting
  static Schema createCustomSchema(@SuppressWarnings("unused") Custom type) {
    return Schema.create(Type.BYTES);
  }

  /**
   * The Tuple is a Record type in Avro. For example, such an Tuple type:
   *
   * <pre>
   *     new Tuple(Native.INT, new Collection(Kind.LIST, Native.TEXT));
   * </pre>
   *
   * will have the following schema:
   *
   * <pre>
   * {
   * "type":"record",
   * "name":"tuple_int_list_text__",
   * "fields":[
   *    {
   *       "name":"t_0",
   *       "type":"int"
   *    },
   *    {
   *       "name":"t_1",
   *       "type":{
   *          "type":"array",
   *          "items":"string"
   *       }
   *    }
   *  ]
   * }
   * </pre>
   *
   * Please note that the name of the record is transformed according to {@link
   * this#tupleToRecordName(Tuple)} method. Every element in the tuple has a name according to
   * {@link this#toTupleFieldName(int)}.
   *
   * <p>The generated schema also supports nested Tuple types.
   */
  @VisibleForTesting
  static Schema creteTupleSchema(Tuple type) {
    FieldAssembler<Schema> tupleSchemaBuilder =
        SchemaBuilder.record(tupleToRecordName(type)).fields();

    for (int i = 0; i < type.getSubTypes().length; i++) {
      tupleSchemaBuilder
          .name(toTupleFieldName(i))
          .type(toAvroType(type.getSubTypes()[i]))
          .noDefault();
    }
    return tupleSchemaBuilder.endRecord();
  }

  /**
   * It creates a name for a given tuple value index. This method is just prepending t_ prefix to an
   * index. For example, for index 0 it will return 't_0'
   */
  public static String toTupleFieldName(int index) {
    return "t_" + index;
  }

  /**
   * It converts the tuple name returned by the {@link Tuple#getName()} to a name that can be used
   * as an Avro record name. For example {@code new Tuple(Native.INT)} will be transformed to
   * 'tuple_int_' string value. It replaces removes all whitespaces, and replaces all occurrences of
   * '<', '>' and ','.
   */
  public static String tupleToRecordName(Tuple type) {
    return StringUtils.deleteWhitespace(type.getName())
        .replaceAll("<", "_")
        .replaceAll(">", "_")
        .replaceAll(",", "_");
  }

  /**
   * The UserDefined is a Record type in Avro. For example, such an UserDefined type:
   *
   * <pre>
   * LinkedHashMap<String, CQLType> udtColumns = new LinkedHashMap<>();
   * udtColumns.put("udtcol_1", Native.INT);
   * udtColumns.put("udtcol_2", Native.TEXT);
   * UserDefined userDefinedType = new UserDefined("ks", "typeName", udtColumns);
   * </pre>
   *
   * will have the following schema:
   *
   * <pre>
   * {
   * "type":"record",
   * "name":"typeName",
   * "fields":[
   *    {
   *       "name":"udtcol_1",
   *       "type":"int"
   *    },
   *    {
   *       "name":"udtcol_2",
   *       "type":"string"
   *    }
   *  ]
   * }
   * </pre>
   *
   * The generated schema also supports nested UserDefined types.
   */
  @VisibleForTesting
  static Schema createUserDefinedSchema(UserDefined type) {
    FieldAssembler<Schema> udtSchemaBuilder = SchemaBuilder.record(type.getName()).fields();
    for (Map.Entry<String, CQLType> udtField : type.getFields().entrySet()) {
      udtSchemaBuilder.name(udtField.getKey()).type(toAvroType(udtField.getValue())).noDefault();
    }
    return udtSchemaBuilder.endRecord();
  }

  /**
   * Avro assumes that every key is of a string type and automatically converts every key to a
   * string representation. See {@link org.apache.avro.util.Utf8} - all keys are converted to this
   * class. Example map with Integer values in avro schema:
   *
   * <pre>
   * {
   *  "type":"map",
   *  "values":{
   *    "type":"map",
   *    "values":"int"
   *   }
   * }
   * </pre>
   *
   * Please note that there is no field that represents the value for keys in a map.
   */
  @VisibleForTesting
  static Schema createMapSchema(MapDataType type) {
    return SchemaBuilder.map().values(toAvroType(type.getValueType()));
  }

  /**
   * Both lists and sets are converted to the Avro array type. For example, the resulting type for
   * set and list of an integer type will be:
   *
   * <pre>
   * {
   *  "type":"array",
   *  "items":"int"
   * }
   * </pre>
   */
  @VisibleForTesting
  static Schema createCollectionSchema(Collection type) {
    return SchemaBuilder.array().items(toAvroType(type.getSubType()));
  }
}
