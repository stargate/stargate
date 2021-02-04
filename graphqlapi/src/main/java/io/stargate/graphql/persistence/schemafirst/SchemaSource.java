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
package io.stargate.graphql.persistence.schemafirst;

import io.stargate.graphql.schema.schemafirst.util.Uuids;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.UUID;

/**
 * A GraphQL schema, as it was provided by the user via a 'deploy' operation.
 *
 * <p>Usually this schema is partial, it will be completed (with default/generated types, directive
 * definitions...) to produce the actual one used at runtime.
 */
public class SchemaSource {
  private final String namespace;
  private final UUID version;
  private final String contents;

  public SchemaSource(String namespace, UUID version, String contents) {
    this.namespace = namespace;
    this.version = version;
    this.contents = contents;
  }

  public String getNamespace() {
    return namespace;
  }

  public UUID getVersion() {
    return version;
  }

  public ZonedDateTime getDeployDate() {
    return ZonedDateTime.ofInstant(
        Instant.ofEpochMilli(Uuids.unixTimestamp(version)), ZoneId.systemDefault());
  }

  public String getContents() {
    return contents;
  }
}
