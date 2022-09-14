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
package io.stargate.sgv2.graphql.persistence.graphqlfirst;

import io.stargate.sgv2.graphql.schema.Uuids;
import io.stargate.sgv2.graphql.web.resources.ResourcePaths;
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
  private final String keyspace;
  private final UUID version;
  private final String contents;

  public SchemaSource(String keyspace, UUID version, String contents) {
    this.keyspace = keyspace;
    this.version = version;
    this.contents = contents;
  }

  public String getKeyspace() {
    return keyspace;
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

  public String getContentsUri() {
    return String.format(
        "%s/keyspace/%s.graphql?version=%s",
        ResourcePaths.FILES_RELATIVE_TO_ADMIN, keyspace, version);
  }

  @Override
  public String toString() {
    return "SchemaSource{"
        + "keyspace='"
        + keyspace
        + '\''
        + ", version="
        + version
        + ", contents='"
        + contents
        + '\''
        + '}';
  }
}
