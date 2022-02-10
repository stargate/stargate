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
package io.stargate.sgv2.common.grpc;

import com.google.protobuf.StringValue;
import io.stargate.proto.Schema.SchemaRead;
import io.stargate.proto.Schema.SchemaRead.ElementType;
import io.stargate.proto.Schema.SchemaRead.SourceApi;

/** Helper methods to construct {@link SchemaRead} instances. */
public class SchemaReads {

  public static SchemaRead keyspace(String keyspaceName, SourceApi sourceApi) {
    return SchemaRead.newBuilder()
        .setSourceApi(sourceApi)
        .setElementType(ElementType.KEYSPACE)
        .setKeyspaceName(keyspaceName)
        .build();
  }

  public static SchemaRead table(String keyspaceName, String tableName, SourceApi sourceApi) {
    return SchemaRead.newBuilder()
        .setSourceApi(sourceApi)
        .setElementType(ElementType.TABLE)
        .setKeyspaceName(keyspaceName)
        .setElementName(StringValue.of(tableName))
        .build();
  }
}
