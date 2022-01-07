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
package io.stargate.sgv2.restsvc.driver;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

/** Re-wraps a {@link PrepareRequest} since it was not designed to modify the payload. */
class ScopedPrepareRequest implements PrepareRequest {

  private final PrepareRequest delegate;
  private final Map<String, ByteBuffer> newCustomPayload;
  private final Map<String, ByteBuffer> newCustomPayloadForBoundStatements;

  public ScopedPrepareRequest(
      PrepareRequest delegate,
      Map<String, ByteBuffer> newPayload,
      Map<String, ByteBuffer> newCustomPayloadForBoundStatements) {
    this.delegate = delegate;
    this.newCustomPayload = newPayload;
    this.newCustomPayloadForBoundStatements = newCustomPayloadForBoundStatements;
  }

  @Override
  @Nullable
  public String getExecutionProfileName() {
    return delegate.getExecutionProfileName();
  }

  @Override
  @Nullable
  public DriverExecutionProfile getExecutionProfile() {
    return delegate.getExecutionProfile();
  }

  @Override
  @Nullable
  public CqlIdentifier getKeyspace() {
    return delegate.getKeyspace();
  }

  @Override
  @Nullable
  public CqlIdentifier getRoutingKeyspace() {
    return delegate.getRoutingKeyspace();
  }

  @Override
  @Nullable
  public ByteBuffer getRoutingKey() {
    return delegate.getRoutingKey();
  }

  @Override
  @Nullable
  public Token getRoutingToken() {
    return delegate.getRoutingToken();
  }

  @Override
  @NonNull
  public Map<String, ByteBuffer> getCustomPayload() {
    return newCustomPayload;
  }

  @Override
  @Nullable
  public Duration getTimeout() {
    return delegate.getTimeout();
  }

  @Override
  @Nullable
  public Node getNode() {
    return delegate.getNode();
  }

  @Override
  @NonNull
  public String getQuery() {
    return delegate.getQuery();
  }

  @Override
  @NonNull
  public Boolean isIdempotent() {
    return delegate.isIdempotent();
  }

  @Override
  @Nullable
  public String getExecutionProfileNameForBoundStatements() {
    return delegate.getExecutionProfileNameForBoundStatements();
  }

  @Override
  @Nullable
  public DriverExecutionProfile getExecutionProfileForBoundStatements() {
    return delegate.getExecutionProfileForBoundStatements();
  }

  @Override
  public CqlIdentifier getRoutingKeyspaceForBoundStatements() {
    return delegate.getRoutingKeyspaceForBoundStatements();
  }

  @Override
  public ByteBuffer getRoutingKeyForBoundStatements() {
    return delegate.getRoutingKeyForBoundStatements();
  }

  @Override
  public Token getRoutingTokenForBoundStatements() {
    return delegate.getRoutingTokenForBoundStatements();
  }

  @Override
  @NonNull
  public Map<String, ByteBuffer> getCustomPayloadForBoundStatements() {
    return newCustomPayloadForBoundStatements;
  }

  @Override
  @Nullable
  public Boolean areBoundStatementsIdempotent() {
    return delegate.areBoundStatementsIdempotent();
  }

  @Override
  @Nullable
  public Duration getTimeoutForBoundStatements() {
    return delegate.getTimeoutForBoundStatements();
  }

  @Override
  public ByteBuffer getPagingStateForBoundStatements() {
    return delegate.getPagingStateForBoundStatements();
  }

  @Override
  public int getPageSizeForBoundStatements() {
    return delegate.getPageSizeForBoundStatements();
  }

  @Override
  @Nullable
  public ConsistencyLevel getConsistencyLevelForBoundStatements() {
    return delegate.getConsistencyLevelForBoundStatements();
  }

  @Override
  @Nullable
  public ConsistencyLevel getSerialConsistencyLevelForBoundStatements() {
    return delegate.getSerialConsistencyLevelForBoundStatements();
  }

  @Override
  public boolean areBoundStatementsTracing() {
    return delegate.areBoundStatementsTracing();
  }
}
