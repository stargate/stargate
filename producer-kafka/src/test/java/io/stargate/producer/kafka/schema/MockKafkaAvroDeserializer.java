package io.stargate.producer.kafka.schema;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import org.apache.avro.Schema;

public class MockKafkaAvroDeserializer extends KafkaAvroDeserializer {
  private final Schema schema;

  public MockKafkaAvroDeserializer(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Object deserialize(String topic, byte[] bytes) {
    this.schemaRegistry = getMockClient(schema);
    return super.deserialize(topic, bytes);
  }

  private static SchemaRegistryClient getMockClient(final Schema schema) {
    return new MockSchemaRegistryClient() {
      @Override
      public synchronized ParsedSchema getSchemaById(int id) {
        return new AvroSchema(schema);
      }

      @Override
      public synchronized ParsedSchema getSchemaBySubjectAndId(String subject, int id)
          throws IOException, RestClientException {
        return super.getSchemaBySubjectAndId(subject, id);
      }
    };
  }
}
