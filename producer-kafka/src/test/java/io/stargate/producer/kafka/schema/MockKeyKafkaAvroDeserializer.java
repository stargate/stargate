package io.stargate.producer.kafka.schema;

public class MockKeyKafkaAvroDeserializer extends MockKafkaAvroDeserializer {
  public MockKeyKafkaAvroDeserializer() {
    super(Schemas.KEY_SCHEMA);
  }
}
