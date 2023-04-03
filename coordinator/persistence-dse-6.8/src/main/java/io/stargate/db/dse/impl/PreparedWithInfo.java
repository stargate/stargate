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
package io.stargate.db.dse.impl;

import org.apache.cassandra.transport.messages.ResultMessage;

public class PreparedWithInfo extends ResultMessage.Prepared {
  private final boolean idempotent;
  private final boolean useKeyspace;

  public PreparedWithInfo(Prepared prepared, boolean idempotent, boolean useKeyspace) {
    super(
        prepared.statementId,
        prepared.resultMetadataId,
        prepared.metadata,
        prepared.resultMetadata);
    this.idempotent = idempotent;
    this.useKeyspace = useKeyspace;
  }

  public boolean isIdempotent() {
    return idempotent;
  }

  public boolean isUseKeyspace() {
    return useKeyspace;
  }
}
