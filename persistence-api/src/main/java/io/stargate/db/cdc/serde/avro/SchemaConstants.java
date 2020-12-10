/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.db.cdc.serde.avro;

import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

public class SchemaConstants {
  public static final String COLUMN_TYPE_ID = "typeId";
  public static final String COLUMN_ORDER = "order";
  public static final String COLUMN_KIND = "kind";
  public static final String COLUMN_NAME = "name";
  public static final String TABLE_NAME = "name";
  public static final String TABLE_COLUMNS = "columns";
  public static final String TABLE_KEYSPACE = "keyspace";
  public static final String MUTATION_EVENT_TABLE = "table";

  public static final Schema COLUMN =
      SchemaBuilder.record("Column")
          .fields()
          .optionalInt(COLUMN_TYPE_ID)
          .optionalString(COLUMN_ORDER)
          .optionalString(COLUMN_KIND)
          .requiredString(COLUMN_NAME)
          .endRecord();

  public static final Schema LIST_OF_COLUMNS = SchemaBuilder.array().items(COLUMN);

  public static final Schema TABLE =
      SchemaBuilder.record("Table")
          .fields()
          .requiredString(TABLE_KEYSPACE)
          .requiredString(TABLE_NAME)
          .name(TABLE_COLUMNS)
          .type(LIST_OF_COLUMNS)
          .withDefault(Collections.emptyList())
          .endRecord();

  public static final String MUTATION_EVENT_TTL = "ttl";
  public static final String MUTATION_EVENT_TIMESTAMP = "timestamp";
  public static final String MUTATION_EVENT_TYPE = "eventType";
  public static final Schema MUTATION_EVENT =
      SchemaBuilder.record("MutationEvent")
          .fields()
          .name(MUTATION_EVENT_TABLE)
          .type(TABLE)
          .noDefault()
          .optionalInt(MUTATION_EVENT_TTL)
          .optionalLong(MUTATION_EVENT_TIMESTAMP)
          .requiredString(MUTATION_EVENT_TYPE)
          .endRecord();
}
