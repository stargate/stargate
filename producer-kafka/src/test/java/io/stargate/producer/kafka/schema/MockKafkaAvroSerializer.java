package io.stargate.producer.kafka.schema;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;

public class MockKafkaAvroSerializer extends KafkaAvroSerializer {
  public MockKafkaAvroSerializer() {
    super();
    super.schemaRegistry = new MockSchemaRegistryClient();
  }

  public MockKafkaAvroSerializer(@SuppressWarnings("unused") SchemaRegistryClient client) {
    super(new MockSchemaRegistryClient());
  }

  public MockKafkaAvroSerializer(
      @SuppressWarnings("unused") SchemaRegistryClient client, Map<String, ?> props) {
    super(new MockSchemaRegistryClient(), props);
  }
}
