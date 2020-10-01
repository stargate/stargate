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

import static org.apache.cassandra.stargate.schema.CQLType.Native.INT;
import static org.apache.cassandra.stargate.schema.CQLType.Native.TEXT;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.cassandra.stargate.schema.CQLType;

public class CqlToAvroTypeConverter {
  public static Schema toAvroType(CQLType type) {
    if (type.equals(TEXT)) {
      return Schema.create(Type.STRING);
    } else if (type.equals(INT)) {
      return Schema.create(Type.INT);
    } else {
      // todo handle other types
      throw new UnsupportedOperationException(String.format("The type: %s is not supported", type));
    }
  }
}
