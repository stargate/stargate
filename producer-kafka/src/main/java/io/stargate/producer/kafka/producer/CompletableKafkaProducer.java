package io.stargate.producer.kafka.producer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompletableKafkaProducer<K, V> extends KafkaProducer<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(CompletableKafkaProducer.class);

  public CompletableKafkaProducer(Map<String, Object> properties) {
    super(properties);
  }

  public CompletableFuture<RecordMetadata> sendAsync(ProducerRecord<K, V> record) {

    final CompletableFuture<RecordMetadata> promise = new CompletableFuture<>();

    final Callback callback =
        (metadata, exception) -> {
          if (exception == null) {
            promise.complete(metadata);
          } else {
            LOG.error("Problem when sending the: " + metadata, exception);
            promise.completeExceptionally(exception);
          }
        };

    super.send(record, callback);

    return promise;
  }
}
