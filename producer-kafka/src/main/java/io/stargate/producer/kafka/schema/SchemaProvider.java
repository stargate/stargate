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

import org.apache.avro.Schema;
import org.apache.cassandra.stargate.schema.TableMetadata;

public interface SchemaProvider {

  /** Returns the avro partition key schema for kafka topic name */
  Schema getKeySchemaForTopic(String topicName);

  /** Returns the avro value schema for kafka topic name */
  Schema getValueSchemaForTopic(String topicName);

  /** Create or update schema for key or/and value if needed */
  void createOrUpdateSchema(TableMetadata tableMetadata);
}
