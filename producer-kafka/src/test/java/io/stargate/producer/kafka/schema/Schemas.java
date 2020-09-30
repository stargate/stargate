package io.stargate.producer.kafka.schema;

import static io.stargate.producer.kafka.schema.KeyValueConstructor.DATA_FIELD_NAME;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.OPERATION_FIELD_NAME;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.TIMESTAMP_FIELD_NAME;
import static io.stargate.producer.kafka.schema.KeyValueConstructor.VALUE_FIELD_NAME;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class Schemas {
  // todo this will be replaced by schema registry calls when the integration with schema registry
  // will be implemented
  public static final String SCHEMA_NAMESPACE = "io.stargate.producer.kafka";
  public static final String KEY_RECORD_NAME = "clusterName.keyspace.table.Key";
  public static final String VALUE_RECORD_NAME = "clusterName.keyspace.table.Value";
  public static final String DATA_RECORD_NAME = "clusterName.keyspace.table.Data";
  public static final String PARTITION_KEY_NAME = "pk_1";

  public static final String COLUMN_NAME = "col_1";

  public static final String CLUSTERING_KEY_NAME = "ck_1";

  // all PKs and Clustering Keys are required (non-optional)
  public static final Schema KEY_SCHEMA =
      SchemaBuilder.record(KEY_RECORD_NAME)
          .namespace(SCHEMA_NAMESPACE)
          .fields()
          .requiredString(PARTITION_KEY_NAME)
          .endRecord();

  public static final Schema VALUE_SCHEMA;

  static {
    Schema timestampMillisType =
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

    Schema partitionKey =
        SchemaBuilder.record(PARTITION_KEY_NAME)
            .fields()
            .requiredString(VALUE_FIELD_NAME)
            .endRecord();
    Schema partitionKeyNullable =
        SchemaBuilder.unionOf().nullType().and().type(partitionKey).endUnion();

    Schema clusteringKey =
        SchemaBuilder.record(CLUSTERING_KEY_NAME)
            .fields()
            .requiredInt(VALUE_FIELD_NAME)
            .endRecord();
    Schema clusteringKeyNullable =
        SchemaBuilder.unionOf().nullType().and().type(clusteringKey).endUnion();

    Schema column =
        SchemaBuilder.record(COLUMN_NAME).fields().optionalString(VALUE_FIELD_NAME).endRecord();
    Schema columnNullable = SchemaBuilder.unionOf().nullType().and().type(column).endUnion();

    Schema fields =
        SchemaBuilder.record(DATA_RECORD_NAME)
            .fields()
            .name(PARTITION_KEY_NAME)
            .type(partitionKeyNullable)
            .noDefault()
            .name(CLUSTERING_KEY_NAME)
            .type(clusteringKeyNullable)
            .noDefault()
            .name(COLUMN_NAME)
            .type(columnNullable)
            .noDefault()
            .endRecord();

    VALUE_SCHEMA =
        SchemaBuilder.record(VALUE_RECORD_NAME)
            .namespace(SCHEMA_NAMESPACE)
            .fields()
            .requiredString(OPERATION_FIELD_NAME)
            .name(TIMESTAMP_FIELD_NAME)
            .type(timestampMillisType)
            .noDefault()
            .name(DATA_FIELD_NAME)
            .type(fields)
            .noDefault()
            .endRecord();
  }
}
