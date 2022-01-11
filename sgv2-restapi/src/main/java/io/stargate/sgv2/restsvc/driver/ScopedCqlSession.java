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
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

  private final CqlSession delegate;
  private final String keyspacePrefix;
  private final Map<String, ByteBuffer> extraPayload;

  public ScopedCqlSession(CqlSession delegate, String token, String tenantId) {
    this.delegate = delegate;
    this.keyspacePrefix = tenantId == null ? null : hexEncode(tenantId) + '_';
    this.extraPayload = PayloadHelper.buildExtraPayload(token, tenantId);
  }

  @Nullable
  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      @NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
    return delegate.execute(PayloadHelper.augment(request, extraPayload), resultType);
  }

  @NonNull
  @Override
  public Metadata getMetadata() {
    Metadata metadata = delegate.getMetadata();
    return keyspacePrefix == null ? metadata : new ScopedMetadata(metadata, keyspacePrefix);
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

  private static String hexEncode(String tenantId) {
    byte[] bytes = tenantId.getBytes(StandardCharsets.UTF_8);
    // Use the driver utility class to convert to hex. It prepends with `0x` so remove it.
    return Bytes.toHexString(bytes).substring(2);
  }
}
