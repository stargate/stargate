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

import com.datastax.oss.protocol.internal.util.Bytes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class BigIntegerConversion extends Conversion<BigInteger> {
  @Override
  public Class<BigInteger> getConvertedType() {
    return BigInteger.class;
  }

  @Override
  public String getLogicalTypeName() {
    return BigIntegerLogicalType.BIG_INTEGER_LOGICAL_TYPE_NAME;
  }

  public BigInteger fromBytes(ByteBuffer bytes, Schema schema, LogicalType type) {
    return (bytes == null) || bytes.remaining() == 0 ? null : new BigInteger(Bytes.getArray(bytes));
  }

  public ByteBuffer toBytes(BigInteger value, Schema schema, LogicalType type) {
    return (value == null) ? null : ByteBuffer.wrap(value.toByteArray());
  }

  @Override
  public Schema getRecommendedSchema() {
    return Schema.create(Type.BYTES);
  }
}
