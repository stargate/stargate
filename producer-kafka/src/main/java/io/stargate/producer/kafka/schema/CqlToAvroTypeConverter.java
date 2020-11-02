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

import static io.stargate.producer.kafka.schema.SchemaConstants.CUSTOM_TYPE_ID;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;
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
import java.util.stream.Collectors;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.StringUtils;

public class CqlToAvroTypeConverter {
  private static final int DEFAULT_DECIMAL_PRECISION = 10;
  private static final Map<Column.Type, Schema> SCHEMA_PER_NATIVE_TYPE = new HashMap<>();

  private CqlToAvroTypeConverter() {}

  static {
    // register custom logical types
    LogicalTypes.register(ShortLogicalType.SHORT_LOGICAL_TYPE_NAME, new ShortTypeFactory());
    LogicalTypes.register(ByteLogicalType.BYTE_LOGICAL_TYPE_NAME, new ByteTypeFactory());
    LogicalTypes.register(
        BigIntegerLogicalType.BIG_INTEGER_LOGICAL_TYPE_NAME, new BigIntegerTypeFactory());

    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Ascii, Schema.create(Type.STRING));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Bigint, Schema.create(Type.LONG));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Blob, Schema.create(Type.BYTES));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Boolean, Schema.create(Type.BOOLEAN));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Counter, Schema.create(Type.LONG));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Date, LogicalTypes.date().addToSchema(Schema.create(Type.INT)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Decimal,
        LogicalTypes.decimal(DEFAULT_DECIMAL_PRECISION).addToSchema(Schema.create(Type.BYTES)));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Double, Schema.create(Type.DOUBLE));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Duration,
        Schema.create(Type.BYTES)); // there is no avro codec for this type, write as raw byte
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Float, Schema.create(Type.FLOAT));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Inet,
        Schema.create(Type.BYTES)); // there is no avro codec for this type, write as raw byte
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Int, Schema.create(Type.INT));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Smallint, new ShortLogicalType().addToSchema(Schema.create(Type.INT)));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Text, Schema.create(Type.STRING));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Time, LogicalTypes.timeMicros().addToSchema(Schema.create(Type.LONG)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Timestamp,
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Type.LONG)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Timeuuid, LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Tinyint, new ByteLogicalType().addToSchema(Schema.create(Type.INT)));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Uuid, LogicalTypes.uuid().addToSchema(Schema.create(Type.STRING)));
    SCHEMA_PER_NATIVE_TYPE.put(Column.Type.Varchar, Schema.create(Type.STRING));
    SCHEMA_PER_NATIVE_TYPE.put(
        Column.Type.Varint, new BigIntegerLogicalType().addToSchema(Schema.create(Type.BYTES)));

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
   * It creates a Schema based on the CqlType. To see how specific types are handled, see the Java
   * doc of other methods.
   */
  public static Schema toAvroType(Column.ColumnType type) {
    if (type.isMap()) {
      return createMapSchema((ParameterizedType.MapType) type);
    } else if (type.isCollection()) {
      return createCollectionSchema(type);
    } else if (type.isUserDefined()) {
      return createUserDefinedSchema((UserDefinedType) type);
    } else if (type.isTuple()) {
      return createTupleSchema((ParameterizedType.TupleType) type);
    } else if (type.id() == CUSTOM_TYPE_ID) {
      return createCustomSchema();
    } else if (type instanceof Column.Type) {
      return getNativeSchema((Column.Type) type);
    } else {
      throw new UnsupportedOperationException(String.format("The type: %s is not supported", type));
    }
  }

  private static Schema getNativeSchema(Column.Type type) {
    return SCHEMA_PER_NATIVE_TYPE.get(type);
  }

  /**
   * The custom type is saved as bytes without an attempt to deserialize it. It's the client
   * responsibility to deserialize it correctly. Currently, the class name is not propagated in the
   * avro message.
   */
  @VisibleForTesting
  static Schema createCustomSchema() {
    return Schema.create(Type.BYTES);
  }

  /**
   * The Tuple is a Record type in Avro. For example, such a Tuple type:
   *
   * <pre>
   *     new Tuple(Column.Type.INT, new Collection(Kind.LIST, Column.Type.TEXT));
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
   * this#tupleToRecordName} method. Every element in the tuple has a name according to {@link
   * this#toTupleFieldName(int)}.
   *
   * <p>The generated schema also supports nested Tuple types.
   */
  @VisibleForTesting
  static Schema createTupleSchema(ParameterizedType.TupleType type) {
    FieldAssembler<Schema> tupleSchemaBuilder =
        SchemaBuilder.record(tupleToRecordName(type)).fields();

    for (int i = 0; i < type.parameters().size(); i++) {
      tupleSchemaBuilder
          .name(toTupleFieldName(i))
          .type(toAvroType(type.parameters().get(i)))
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
   * It converts the tuple name returned by the {@link ParameterizedType.TupleType#cqlDefinition()}
   * to a name that can be used as an Avro record name. For example {@code new Tuple(Column.Type
   * .INT)} will be transformed to 'tuple_int_' string value. It replaces removes all whitespaces,
   * and replaces all occurrences of '<', '>' and ','.
   */
  public static String tupleToRecordName(ParameterizedType.TupleType type) {

    return StringUtils.deleteWhitespace(
        type.rawType().cqlDefinition()
            + type.parameters().stream()
                .map(p -> p.cqlDefinition())
                .collect(Collectors.joining("_", "_", "_"))
                .replaceAll("<", "_")
                .replaceAll(">", "_")
                .replaceAll(",", "_"));
  }

  /**
   * The UserDefined is a Record type in Avro. For example, such an UserDefined type:
   *
   * <pre>
   * LinkedHashMap<String, CQLType> udtColumns = new LinkedHashMap<>();
   * udtColumns.put("udtcol_1", Column.Type.Int);
   * udtColumns.put("udtcol_2", Column.Type.Text);
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
  static Schema createUserDefinedSchema(UserDefinedType type) {
    FieldAssembler<Schema> udtSchemaBuilder = SchemaBuilder.record(type.name()).fields();
    for (Column udtField : type.columns()) {
      udtSchemaBuilder.name(udtField.name()).type(toAvroType(udtField.type())).noDefault();
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
  static Schema createMapSchema(ParameterizedType.MapType type) {
    return SchemaBuilder.map().values(toAvroType(type.parameters().get(1)));
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
  static Schema createCollectionSchema(Column.ColumnType type) {
    return SchemaBuilder.array().items(toAvroType(type.parameters().get(0)));
  }
}
