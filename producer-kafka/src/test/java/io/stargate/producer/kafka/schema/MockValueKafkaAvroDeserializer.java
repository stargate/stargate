package io.stargate.producer.kafka.schema;

public class MockValueKafkaAvroDeserializer extends MockKafkaAvroDeserializer {
  public MockValueKafkaAvroDeserializer() {
    super(Schemas.VALUE_SCHEMA);
  }
}
