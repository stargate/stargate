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

public class SchemaConstants {
  public static final String OPERATION_FIELD_NAME = "op";
  public static final String TIMESTAMP_FIELD_NAME = "ts_ms";
  public static final String DATA_FIELD_NAME = "data";
  public static final String VALUE_FIELD_NAME = "value";
  public static final int CUSTOM_TYPE_ID = 0;
}
