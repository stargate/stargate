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

import static io.stargate.producer.kafka.helpers.MutationEventHelper.clusteringKey;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.column;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createDeleteEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createDeleteEventNoCk;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createDeleteEventNoPk;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEvent;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEventNoCK;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEventNoColumns;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.createRowUpdateEventNoPk;
import static io.stargate.producer.kafka.helpers.MutationEventHelper.partitionKey;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.OPERATION_FIELD_NAME;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.TIMESTAMP_FIELD_NAME;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.VALUE_FIELD_NAME;
import static io.stargate.producer.kafka.schema.Schemas.CLUSTERING_KEY_NAME;
import static io.stargate.producer.kafka.schema.Schemas.COLUMN_NAME;
import static io.stargate.producer.kafka.schema.Schemas.KEY_SCHEMA;
import static io.stargate.producer.kafka.schema.Schemas.PARTITION_KEY_NAME;
import static io.stargate.producer.kafka.schema.Schemas.VALUE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.cassandra.stargate.db.DeleteEvent;
import org.apache.cassandra.stargate.db.MutationEvent;
import org.apache.cassandra.stargate.db.RowUpdateEvent;
import org.apache.cassandra.stargate.schema.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KeyValueConstructorTest {

  private static final String TOPIC_NAME = "t1";

  @Test
  public void shouldConstructKey() {
    // given
    SchemaProvider schemaProvider = mock(SchemaProvider.class);

    KeyValueConstructor keyValueConstructor = new KeyValueConstructor(schemaProvider);
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 100;
    MutationEvent rowMutationEvent =
        createRowUpdateEvent(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME),
            null,
            column(COLUMN_NAME),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME),
            mock(TableMetadata.class));
    when(schemaProvider.getKeySchemaForTopic(TOPIC_NAME)).thenReturn(Schemas.KEY_SCHEMA);

    // when
    GenericRecord genericRecord = keyValueConstructor.constructKey(rowMutationEvent, TOPIC_NAME);

    // then
    GenericRecord expected = new GenericData.Record(KEY_SCHEMA);
    expected.put(PARTITION_KEY_NAME, partitionKeyValue);
    assertThat(genericRecord).isEqualTo(expected);
    assertThatCode(() -> validateThatCanWrite(genericRecord, KEY_SCHEMA))
        .doesNotThrowAnyException();
  }

  @Test
  public void shouldThrowWhenConstructingKeyWithNullValue() {
    // given
    SchemaProvider schemaProvider = mock(SchemaProvider.class);

    KeyValueConstructor keyValueConstructor = new KeyValueConstructor(schemaProvider);
    RowUpdateEvent rowMutationEvent =
        createRowUpdateEvent(
            null,
            partitionKey(PARTITION_KEY_NAME),
            null,
            column(COLUMN_NAME),
            null,
            clusteringKey(CLUSTERING_KEY_NAME),
            mock(TableMetadata.class));
    when(schemaProvider.getKeySchemaForTopic(TOPIC_NAME)).thenReturn(Schemas.KEY_SCHEMA);
    // when
    GenericRecord genericRecord = keyValueConstructor.constructKey(rowMutationEvent, TOPIC_NAME);

    // then
    assertThatThrownBy(() -> validateThatCanWrite(genericRecord, KEY_SCHEMA))
        .hasMessageContaining(String.format("null of string in field %s", PARTITION_KEY_NAME));
  }

  @ParameterizedTest
  @MethodSource("updateValueProvider")
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public void shouldConstructUpdateValue(
      String partitionKeyValue,
      Integer clusteringKeyValue,
      String columnValue,
      Optional<String> exceptionMessage) {
    // given
    SchemaProvider schemaProvider = mock(SchemaProvider.class);

    KeyValueConstructor keyValueConstructor = new KeyValueConstructor(schemaProvider);
    long timestamp = 1000;
    RowUpdateEvent rowMutationEvent =
        createRowUpdateEvent(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME),
            columnValue,
            column(COLUMN_NAME),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME),
            mock(TableMetadata.class),
            timestamp);
    when(schemaProvider.getValueSchemaForTopic(TOPIC_NAME)).thenReturn(Schemas.VALUE_SCHEMA);

    // when
    GenericRecord genericRecord = keyValueConstructor.constructValue(rowMutationEvent, TOPIC_NAME);

    // then
    assertThat(genericRecord.get(OPERATION_FIELD_NAME)).isEqualTo(OperationType.UPDATE.getAlias());
    assertThat(genericRecord.get(TIMESTAMP_FIELD_NAME)).isEqualTo(timestamp);
    Record data = (Record) genericRecord.get(DATA_FIELD_NAME);
    assertThat(getFieldValue(data, PARTITION_KEY_NAME)).isEqualTo(partitionKeyValue);
    assertThat(getFieldValue(data, CLUSTERING_KEY_NAME)).isEqualTo(clusteringKeyValue);
    assertThat(getFieldValue(data, COLUMN_NAME)).isEqualTo(columnValue);
    if (exceptionMessage.isPresent()) {
      // write should fail because some required field is missing
      assertThatThrownBy(() -> validateThatCanWrite(genericRecord, VALUE_SCHEMA))
          .hasMessageContaining(exceptionMessage.get());
    } else {
      assertThatCode(() -> validateThatCanWrite(genericRecord, VALUE_SCHEMA))
          .doesNotThrowAnyException();
    }
  }

  @ParameterizedTest
  @MethodSource("deleteValueProvider")
  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public void shouldConstructDeleteValue(
      String partitionKeyValue, Integer clusteringKeyValue, Optional<String> exceptionMessage) {
    // given
    SchemaProvider schemaProvider = mock(SchemaProvider.class);

    KeyValueConstructor keyValueConstructor = new KeyValueConstructor(schemaProvider);
    long timestamp = 1000;
    MutationEvent event =
        createDeleteEvent(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME),
            mock(TableMetadata.class),
            timestamp);
    when(schemaProvider.getValueSchemaForTopic(TOPIC_NAME)).thenReturn(Schemas.VALUE_SCHEMA);

    // when
    GenericRecord genericRecord = keyValueConstructor.constructValue(event, TOPIC_NAME);

    // then
    assertThat(genericRecord.get(OPERATION_FIELD_NAME)).isEqualTo(OperationType.DELETE.getAlias());
    assertThat(genericRecord.get(TIMESTAMP_FIELD_NAME)).isEqualTo(timestamp);
    Record data = (Record) genericRecord.get(DATA_FIELD_NAME);
    assertThat(getFieldValue(data, PARTITION_KEY_NAME)).isEqualTo(partitionKeyValue);
    assertThat(getFieldValue(data, CLUSTERING_KEY_NAME)).isEqualTo(clusteringKeyValue);
    if (exceptionMessage.isPresent()) {
      // write should fail because some required field is missing
      assertThatThrownBy(() -> validateThatCanWrite(genericRecord, VALUE_SCHEMA))
          .hasMessageContaining(exceptionMessage.get());
    } else {
      assertThatCode(() -> validateThatCanWrite(genericRecord, VALUE_SCHEMA))
          .doesNotThrowAnyException();
    }
  }

  @ParameterizedTest
  @MethodSource("valueProviderOmittedFields")
  public void shouldAllowOmittingField(MutationEvent rowMutationEvent, String nullColumnName) {
    // given
    SchemaProvider schemaProvider = mock(SchemaProvider.class);
    KeyValueConstructor keyValueConstructor = new KeyValueConstructor(schemaProvider);
    when(schemaProvider.getValueSchemaForTopic(TOPIC_NAME)).thenReturn(Schemas.VALUE_SCHEMA);

    // when
    GenericRecord genericRecord = keyValueConstructor.constructValue(rowMutationEvent, TOPIC_NAME);

    // then
    Record data = (Record) genericRecord.get(DATA_FIELD_NAME);
    assertThat(data.get(nullColumnName)).isNull();
    assertThatCode(() -> validateThatCanWrite(genericRecord, VALUE_SCHEMA))
        .doesNotThrowAnyException();
  }

  private static Stream<Arguments> valueProviderOmittedFields() {
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 100;
    String columnValue = "col_value";

    RowUpdateEvent rowUpdateEventNoPK =
        createRowUpdateEventNoPk(
            columnValue,
            column(COLUMN_NAME),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME),
            mock(TableMetadata.class));

    RowUpdateEvent rowUpdateEventNoCK =
        createRowUpdateEventNoCK(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME),
            columnValue,
            column(COLUMN_NAME),
            mock(TableMetadata.class));

    RowUpdateEvent rowUpdateEventNoColumns =
        createRowUpdateEventNoColumns(
            partitionKeyValue,
            partitionKey(PARTITION_KEY_NAME),
            clusteringKeyValue,
            clusteringKey(CLUSTERING_KEY_NAME),
            mock(TableMetadata.class));

    DeleteEvent deleteEventNoPK =
        createDeleteEventNoPk(
            clusteringKeyValue, clusteringKey(CLUSTERING_KEY_NAME), mock(TableMetadata.class));

    DeleteEvent deleteEventNoCK =
        createDeleteEventNoCk(
            partitionKeyValue, clusteringKey(PARTITION_KEY_NAME), mock(TableMetadata.class));

    return Stream.of(
        Arguments.of(rowUpdateEventNoPK, PARTITION_KEY_NAME),
        Arguments.of(rowUpdateEventNoCK, CLUSTERING_KEY_NAME),
        Arguments.of(rowUpdateEventNoColumns, COLUMN_NAME),
        Arguments.of(deleteEventNoPK, PARTITION_KEY_NAME),
        Arguments.of(deleteEventNoCK, CLUSTERING_KEY_NAME));
  }

  private static Stream<Arguments> updateValueProvider() {
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 100;
    String columnValue = "col_value";
    Optional<String> noError = Optional.empty();
    Optional<String> missingPK = Optional.of(missingRequiredFieldStringValue(PARTITION_KEY_NAME));
    Optional<String> missingCK = Optional.of(missingRequiredFieldIntValue(CLUSTERING_KEY_NAME));

    return Stream.of(
        Arguments.of(partitionKeyValue, clusteringKeyValue, columnValue, noError),
        Arguments.of(partitionKeyValue, clusteringKeyValue, null, noError), // null column value
        Arguments.of(partitionKeyValue, null, columnValue, missingCK), // null clustering key value
        Arguments.of(null, clusteringKeyValue, columnValue, missingPK)); // null partition key value
  }

  private static Stream<Arguments> deleteValueProvider() {
    String partitionKeyValue = "pk_value";
    Integer clusteringKeyValue = 100;
    Optional<String> noError = Optional.empty();
    Optional<String> missingPK =
        Optional.of(missingRequiredFieldValue(PARTITION_KEY_NAME, "string"));
    Optional<String> missingCK = Optional.of(missingRequiredFieldValue(CLUSTERING_KEY_NAME, "int"));

    return Stream.of(
        Arguments.of(partitionKeyValue, clusteringKeyValue, noError),
        Arguments.of(partitionKeyValue, null, missingCK), // null clustering key value
        Arguments.of(null, clusteringKeyValue, missingPK)); // null partition key value
  }

  private Object getFieldValue(Record data, String fieldName) {
    return ((Record) data.get(fieldName)).get(VALUE_FIELD_NAME);
  }

  /** Validates that the GenericRecord complies with the schema. */
  private void validateThatCanWrite(GenericRecord genericRecord, Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    EncoderFactory encoderFactory = EncoderFactory.get();
    BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(genericRecord, encoder);
  }

  private static String missingRequiredFieldStringValue(String fieldName) {
    return missingRequiredFieldValue(fieldName, "string");
  }

  private static String missingRequiredFieldIntValue(String fieldName) {
    return missingRequiredFieldValue(fieldName, "int");
  }

  private static String missingRequiredFieldValue(String fieldName, String type) {
    return String.format("null of %s in field value of %s", type, fieldName);
  }
}
