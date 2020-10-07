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
package io.stargate.producer.kafka.schema.codecs;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.LogicalTypeFactory;
import org.apache.avro.Schema;

public class ShortLogicalType extends LogicalType {
  public static final String SHORT_DURATION_LOGICAL_TYPE_NAME = "short";

  public ShortLogicalType() {
    super(SHORT_DURATION_LOGICAL_TYPE_NAME);
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    if (schema.getType() != Schema.Type.INT) {
      throw new IllegalArgumentException("Logical type 'short' must be backed by int");
    }
  }

  public static class ShortTypeFactory implements LogicalTypeFactory {
    private static LogicalType SHORT_LOGICAL_TYPE = new ShortLogicalType();

    @Override
    public LogicalType fromSchema(Schema schema) {
      return SHORT_LOGICAL_TYPE;
    }

    @Override
    public String getTypeName() {
      return ShortLogicalType.SHORT_DURATION_LOGICAL_TYPE_NAME;
    }
  }
}
