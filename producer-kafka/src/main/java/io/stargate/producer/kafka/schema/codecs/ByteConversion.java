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

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class ByteConversion extends Conversion<Byte> {
  @Override
  public Class<Byte> getConvertedType() {
    return Byte.class;
  }

  @Override
  public String getLogicalTypeName() {
    return ByteLogicalType.BYTE_LOGICAL_TYPE_NAME;
  }

  @Override
  public Byte fromInt(Integer value, Schema schema, LogicalType type) {
    return value.byteValue();
  }

  @Override
  public Integer toInt(Byte value, Schema schema, LogicalType type) {
    return value.intValue();
  }

  @Override
  public Schema getRecommendedSchema() {
    return Schema.create(Type.INT);
  }
}
