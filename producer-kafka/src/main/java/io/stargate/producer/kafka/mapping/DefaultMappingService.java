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
package io.stargate.producer.kafka.mapping;

import io.stargate.db.schema.Table;
import io.stargate.producer.kafka.configuration.ConfigLoader;

public class DefaultMappingService implements MappingService {
  private final String prefixName;

  public DefaultMappingService(String prefixName) {
    this.prefixName = prefixName;
  }

  /**
   * It constructs the topic name to which the CDC modification for a given TableMetadata is sent.
   * It uses the {@link ConfigLoader#CDC_TOPIC_PREFIX_NAME} as the prefix. For the prefix 'p',
   * keyspace 'ks' and table 't', the final topic name will be: 'p.ks.t'
   */
  @Override
  public String getTopicNameFromTableMetadata(Table table) {
    return String.format("%s.%s.%s", prefixName, table.keyspace(), table.name());
  }
}
