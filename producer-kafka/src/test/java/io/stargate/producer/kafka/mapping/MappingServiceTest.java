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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.db.schema.Table;
import org.junit.jupiter.api.Test;

class MappingServiceTest {

  @Test
  public void shouldConstructMappingForPrefix() {
    // given
    DefaultMappingService mappingService = new DefaultMappingService("prefix");
    Table tableMetadata = mock(Table.class);
    when(tableMetadata.keyspace()).thenReturn("k1");
    when(tableMetadata.name()).thenReturn("table");

    // when
    String topicNameFromTableMetadata = mappingService.getTopicNameFromTableMetadata(tableMetadata);

    // then
    assertThat(topicNameFromTableMetadata).isEqualTo("prefix.k1.table");
  }
}
