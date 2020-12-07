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
import static io.stargate.producer.kafka.schema.SchemaConstants.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.OPERATION_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.TIMESTAMP_FIELD_NAME;
import static io.stargate.producer.kafka.schema.SchemaConstants.VALUE_FIELD_NAME;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ParameterizedType;
import io.stargate.db.schema.UserDefinedType;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;

public class KeyValueConstructor {

  private static final List<Class<?>> AVRO_UNSUPPORTED_TYPES =
      Arrays.asList(CqlDuration.class, InetAddress.class);

  private SchemaProvider schemaProvider;

  public KeyValueConstructor(SchemaProvider schemaProvider) {
    this.schemaProvider = schemaProvider;
  }

  @NonNull
  public GenericRecord constructValue(MutationEvent mutationEvent, String topicName) {
    Schema schema = schemaProvider.getValueSchemaForTopic(topicName);
    GenericRecord value = new GenericData.Record(schema);

    Schema dataSchema =
        schema.getFields().stream()
            .filter(f -> f.name().equals(DATA_FIELD_NAME))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        String.format(
                            "There is no %s field in the record schema for topic: %s.",
                            DATA_FIELD_NAME, topicName)))
            .schema();

    GenericRecord data = new Record(dataSchema);

    createUnionAndAppendToData(mutationEvent.getPartitionKeys(), dataSchema, data);
    createUnionAndAppendToData(mutationEvent.getClusteringKeys(), dataSchema, data);
    if (mutationEvent instanceof RowUpdateEvent) {
      createUnionAndAppendToData(((RowUpdateEvent) mutationEvent).getCells(), dataSchema, data);
      value.put(OPERATION_FIELD_NAME, OperationType.UPDATE.getAlias());
    } else if (mutationEvent instanceof DeleteEvent) {
      value.put(OPERATION_FIELD_NAME, OperationType.DELETE.getAlias());
    }

    value.put(TIMESTAMP_FIELD_NAME, mutationEvent.getTimestamp());
    value.put(DATA_FIELD_NAME, data);

    return value;
  }

  /** All Partition Keys must be included in the kafka.key */
  @NonNull
  public GenericRecord constructKey(MutationEvent mutationEvent, String topicName) {
    GenericRecord key = new GenericData.Record(schemaProvider.getKeySchemaForTopic(topicName));
    fillGenericRecordWithData(mutationEvent.getPartitionKeys(), key);
    return key;
  }

  private void createUnionAndAppendToData(
      List<? extends CellValue> cellValues, Schema dataSchema, GenericRecord data) {
    cellValues.forEach(
        v -> {
          String columnName = v.getColumn().name();
          Schema unionSchema = dataSchema.getField(columnName).schema();
          GenericRecord record = validateUnionTypeAndConstructRecord(columnName, unionSchema);

          if (v.getColumn().type().isUserDefined()) {
            handleUdt(v, record);
          } else if (v.getColumn().type().isTuple()) {
            handleTuple(v, record);
          } else {
            record.put(VALUE_FIELD_NAME, getValueObjectOrByteBuffer(v));
          }
          data.put(columnName, record);
        });
  }

  private void handleUdt(CellValue v, GenericRecord record) {
    Schema udtSchema = validateNumberOfFieldsAndConstructSchema(v, record);
    UserDefinedType userDefined = (UserDefinedType) v.getColumn().type();
    GenericRecord innerRecord = validateUnionTypeAndConstructRecord(userDefined.name(), udtSchema);

    record.put(VALUE_FIELD_NAME, constructUdt(userDefined, v.getValueObject(), innerRecord));
  }

  private void handleTuple(CellValue v, GenericRecord record) {
    Schema tupleSchema = validateNumberOfFieldsAndConstructSchema(v, record);
    ParameterizedType.TupleType tuple = (ParameterizedType.TupleType) v.getColumn().type();
    GenericRecord innerRecord =
        validateUnionTypeAndConstructRecord(
            CqlToAvroTypeConverter.tupleToRecordName(tuple), tupleSchema);

    record.put(VALUE_FIELD_NAME, constructTuple(tuple, v.getValueObject(), innerRecord));
  }

  private Schema validateNumberOfFieldsAndConstructSchema(CellValue v, GenericRecord record) {
    List<Field> fields = record.getSchema().getFields();
    if (fields.size() > 1) {
      throw new IllegalStateException(
          "The schema for: "
              + v.getColumn()
              + " should have only one field, but has: "
              + fields.size());
    }
    return fields.get(0).schema();
  }

  @SuppressWarnings("unchecked")
  private Object constructTuple(
      @NonNull ParameterizedType.TupleType tuple,
      @NonNull Object value,
      @NonNull GenericRecord record) {

    if (!(value instanceof List)) {
      throw new IllegalArgumentException(
          "The underlying type of a Tuple should be a list, but is:" + value.getClass());
    }
    List<Object> udtValues = (List<Object>) value;
    for (int i = 0; i < udtValues.size(); i++) {
      Object udtValue = udtValues.get(i);
      String udtKey = CqlToAvroTypeConverter.toTupleFieldName(i);
      Column.ColumnType cqlType = tuple.parameters().get(i);
      if (cqlType instanceof ParameterizedType.TupleType) {
        // extract nested value
        Record nestedUdtRecord = new Record(record.getSchema().getField(udtKey).schema());
        constructTuple((ParameterizedType.TupleType) cqlType, udtValue, nestedUdtRecord);
        record.put(udtKey, nestedUdtRecord);
      } else {
        // put actual value
        record.put(udtKey, udtValue);
      }
    }
    return record;
  }

  @SuppressWarnings("unchecked")
  private Object constructUdt(
      @NonNull UserDefinedType userDefined, @NonNull Object value, @NonNull GenericRecord record) {
    if (!(value instanceof Map)) {
      throw new IllegalArgumentException(
          "The underlying type of a UserDefined should be a Map, but is:" + value.getClass());
    }
    Map<String, Object> udtValues = (Map<String, Object>) value;
    for (Map.Entry<String, Object> udtValue : udtValues.entrySet()) {
      Map<String, Column> columnMap = userDefined.columnMap();
      Column.ColumnType cqlType = columnMap.get(udtValue.getKey()).type();
      if (cqlType.isUserDefined()) {
        // extract nested values
        Record nestedUdtRecord =
            new Record(record.getSchema().getField(udtValue.getKey()).schema());
        constructUdt((UserDefinedType) cqlType, udtValue.getValue(), nestedUdtRecord);
        record.put(udtValue.getKey(), nestedUdtRecord);
      } else {
        // put actual value
        record.put(udtValue.getKey(), udtValue.getValue());
      }
    }
    return record;
  }

  private GenericRecord validateUnionTypeAndConstructRecord(String columnName, Schema unionSchema) {
    if (!unionSchema.getType().equals(Type.UNION)) {
      throw new IllegalStateException(
          String.format(
              "The type for %s should be UNION but is: %s", columnName, unionSchema.getType()));
    }
    return new Record(
        unionSchema.getTypes().get(1)); // 0 - is null type, 1 - is an actual union type
  }

  private void fillGenericRecordWithData(
      List<? extends CellValue> cellValues, GenericRecord genericRecord) {
    cellValues.forEach(
        cellValue ->
            genericRecord.put(cellValue.getColumn().name(), getValueObjectOrByteBuffer(cellValue)));
  }

  /**
   * It returns the java representation of the underlying value using {@link
   * CellValue#getValueObject()}. For the {@link CqlDuration} and {@link InetAddress} it returns the
   * byte buffer using {@link CellValue#getValue()} because both of those type does not have an avro
   * representation. If the cell value is of a custom type it also returns byte buffer.
   */
  private Object getValueObjectOrByteBuffer(CellValue valueObject) {
    if (valueObject.getValueObject() == null) {
      return null;
    }

    // custom type should be saved as bytes
    if (valueObject.getColumn().type().id() == CUSTOM_TYPE_ID) {
      valueObject.getValue();
    }

    for (Class<?> avroUnsupportedType : AVRO_UNSUPPORTED_TYPES) {
      if (avroUnsupportedType.isAssignableFrom(valueObject.getValueObject().getClass())) {
        return valueObject.getValue();
      }
    }
    return valueObject.getValueObject();
  }
}
