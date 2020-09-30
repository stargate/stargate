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
package io.stargate.db;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.transport.ProtocolVersion;

public interface QueryOptions {
  ConsistencyLevel getConsistency();

  List<ByteBuffer> getValues();

  List<String> getNames();

  ProtocolVersion getProtocolVersion();

  int getPageSize();

  ByteBuffer getPagingState();

  ConsistencyLevel getSerialConsistency();

  long getTimestamp();

  int getNowInSeconds();

  String getKeyspace();

  boolean skipMetadata();
}
