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

import io.stargate.db.schema.Table;
import org.apache.avro.Schema;

public interface SchemaProvider {

  /**
   * Returns the avro partition key schema for kafka topic name. It must be called only after the
   * createOrUpdateSchema returned successfully.
   */
  Schema getKeySchemaForTopic(String topicName);

  /**
   * Returns the avro value schema for kafka topic name It must be called only after the
   * createOrUpdateSchema returned successfully.
   */
  Schema getValueSchemaForTopic(String topicName);

  /**
   * Creates or updates schema for key or/and value if needed. The caller of this method should
   * assert that there is a happens-before relation between calling createOrUpdateSchema and
   * getKeySchemaForTopic or getValueSchemaForTopic. In other words, the createOrUpdateSchema must
   * return successfully before calling both get methods.
   */
  void createOrUpdateSchema(Table table);
}
