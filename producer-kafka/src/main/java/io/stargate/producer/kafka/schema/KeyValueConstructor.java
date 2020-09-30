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

import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.stargate.db.CellValue;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.jetbrains.annotations.NotNull;

public class KeyValueConstructor {

  private SchemaProvider schemaProvider;
  public static final String OPERATION_FIELD_NAME = "op";
  public static final String TIMESTAMP_FIELD_NAME = "ts_ms";
  public static final String DATA_FIELD_NAME = "data";
  public static final String VALUE_FIELD_NAME = "value";

  public KeyValueConstructor(SchemaProvider schemaProvider) {
    this.schemaProvider = schemaProvider;
  }

  @NotNull
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
  @NotNull
  public GenericRecord constructKey(MutationEvent mutationEvent, String topicName) {
    GenericRecord key = new GenericData.Record(schemaProvider.getKeySchemaForTopic(topicName));
    fillGenericRecordWithData(mutationEvent.getPartitionKeys(), key);
    return key;
  }

  private void createUnionAndAppendToData(
      List<? extends CellValue> cellValues, Schema dataSchema, GenericRecord data) {
    cellValues.forEach(
        pk -> {
          String columnName = pk.getColumn().getName();
          Schema unionSchema = dataSchema.getField(columnName).schema();
          if (!unionSchema.getType().equals(Type.UNION)) {
            throw new IllegalStateException(
                String.format(
                    "The type for %s should be UNION but is: %s",
                    columnName, unionSchema.getType()));
          }
          GenericRecord record =
              new Record(
                  unionSchema.getTypes().get(1)); // 0 - is null type, 1 - is an actual union type
          record.put(VALUE_FIELD_NAME, pk.getValueObject());
          data.put(columnName, record);
        });
  }

  private void fillGenericRecordWithData(
      List<? extends CellValue> cellValues, GenericRecord genericRecord) {
    cellValues.forEach(
        cellValue -> {
          genericRecord.put(cellValue.getColumn().getName(), cellValue.getValueObject());
        });
  }
}
