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

public class ByteLogicalType extends LogicalType {
  public static final String BYTE_LOGICAL_TYPE_NAME = "byte";

  public ByteLogicalType() {
    super(BYTE_LOGICAL_TYPE_NAME);
  }

  @Override
  public void validate(Schema schema) {
    super.validate(schema);
    if (schema.getType() != Schema.Type.INT) {
      throw new IllegalArgumentException(
          String.format("Logical type '%s' must be backed by int", BYTE_LOGICAL_TYPE_NAME));
    }
  }

  public static class ByteTypeFactory implements LogicalTypeFactory {
    private static LogicalType BYTE_LOGICAL_TYPE = new ByteLogicalType();

    @Override
    public LogicalType fromSchema(Schema schema) {
      return BYTE_LOGICAL_TYPE;
    }

    @Override
    public String getTypeName() {
      return ByteLogicalType.BYTE_LOGICAL_TYPE_NAME;
    }
  }
}
