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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Wraps a {@link CqlSession} for a particular auth token and/or tenant ID.
 *
 * <p>Every statement executed through that session will be authenticated and scoped to that tenant
 * (this is implemented by forwarding that information to the CQL bridge via the custom payload).
 */
public class ScopedCqlSession implements CqlSession {

  // TODO shared with PersistenceConnectionFactory in the cql module, maybe find a way to factor
  private static final String TOKEN_PAYLOAD_KEY = "stargate.bridge.token";
  private static final String TENANT_PAYLOAD_KEY = "stargate.bridge.tenantId";

  private final CqlSession delegate;
  private final String tenantId;
  private final Map<String, ByteBuffer> extraPayload;

  public ScopedCqlSession(CqlSession delegate, String token, String tenantId) {
    this.delegate = delegate;
    this.tenantId = tenantId;
    this.extraPayload = buildExtraPayload(token, tenantId);
  }

  @Nullable
  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      @NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
    return delegate.execute(augment(request, extraPayload), resultType);
  }

  @NonNull
  @Override
  public Metadata getMetadata() {
    // TODO filter keyspaces by tenantId
    return delegate.getMetadata();
  }

  @NonNull
  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return delegate.isSchemaMetadataEnabled();
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
    return delegate.setSchemaMetadataEnabled(newValue);
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return delegate.refreshSchemaAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return delegate.checkSchemaAgreementAsync();
  }

  @NonNull
  @Override
  public DriverContext getContext() {
    return delegate.getContext();
  }

  @NonNull
  @Override
  public Optional<CqlIdentifier> getKeyspace() {
    return delegate.getKeyspace();
  }

  @NonNull
  @Override
  public Optional<Metrics> getMetrics() {
    return delegate.getMetrics();
  }

  private static Map<String, ByteBuffer> buildExtraPayload(String token, String tenantId) {
    assert token != null || tenantId != null;
    Map<String, ByteBuffer> payload = new HashMap<>();
    if (token != null) {
      payload.put(TOKEN_PAYLOAD_KEY, StandardCharsets.UTF_8.encode(token));
    }
    if (tenantId != null) {
      payload.put(TENANT_PAYLOAD_KEY, StandardCharsets.UTF_8.encode(tenantId));
    }
    return Collections.unmodifiableMap(payload);
  }

  private static <RequestT extends Request> RequestT augment(
      RequestT request, Map<String, ByteBuffer> extraPayload) {
    if (request instanceof Statement) {
      Statement<?> statement = (Statement<?>) request;
      Map<String, ByteBuffer> newPayload = augment(statement.getCustomPayload(), extraPayload);
      statement = statement.setCustomPayload(newPayload);
      // The cast is safe because setCustomPayload returns the statement's self type
      @SuppressWarnings("unchecked")
      RequestT newRequest = (RequestT) statement;
      return newRequest;
    } else if (request instanceof PrepareRequest) {
      PrepareRequest prepare = (PrepareRequest) request;
      Map<String, ByteBuffer> newPayload = augment(prepare.getCustomPayload(), extraPayload);
      Map<String, ByteBuffer> newPayloadForBoundStatements =
          augment(prepare.getCustomPayloadForBoundStatements(), extraPayload);
      @SuppressWarnings("unchecked")
      RequestT newRequest =
          (RequestT) new ScopedPrepareRequest(prepare, newPayload, newPayloadForBoundStatements);
      return newRequest;
    } else {
      // That's all we should encounter for CQL
      throw new UnsupportedOperationException(
          "Unexpected request type " + request.getClass().getName());
    }
  }

  private static Map<String, ByteBuffer> augment(
      Map<String, ByteBuffer> payload, Map<String, ByteBuffer> toAdd) {
    if (payload == null || payload.isEmpty()) {
      return toAdd;
    } else {
      try {
        payload.putAll(toAdd);
        return payload;
      } catch (UnsupportedOperationException e) {
        Map<String, ByteBuffer> newPayload = new HashMap<>(payload);
        newPayload.putAll(toAdd);
        return newPayload;
      }
    }
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return delegate.closeFuture();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    throw new UnsupportedOperationException("This wrapper is not meant to be closed");
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    throw new UnsupportedOperationException("This wrapper is not meant to be closed");
  }
}
