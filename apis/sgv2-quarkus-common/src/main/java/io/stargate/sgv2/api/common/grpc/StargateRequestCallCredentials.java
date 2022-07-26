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
package io.stargate.sgv2.api.common.grpc;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;

import java.util.Optional;
import java.util.concurrent.Executor;

public class StargateRequestCallCredentials extends CallCredentials {

    private final Metadata.Key<String> tenantIdKey;
    private final Metadata.Key<String> cassandraTokenKey;
    private final Optional<String> tenantId;
    private final Optional<String> cassandraToken;

    public StargateRequestCallCredentials(
            Metadata.Key<String> tenantIdKey,
            Metadata.Key<String> cassandraTokenKey,
            Optional<String> tenantId,
            Optional<String> cassandraToken
    ) {
        this.tenantIdKey = tenantIdKey;
        this.cassandraTokenKey = cassandraTokenKey;
        this.tenantId = tenantId;
        this.cassandraToken = cassandraToken;
    }

    @Override
    public void applyRequestMetadata(
            RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        appExecutor.execute(
                () -> {
                    try {
                        Metadata metadata = new Metadata();
                        tenantId.ifPresent(t -> metadata.put(tenantIdKey, t));
                        cassandraToken.ifPresent(t -> metadata.put(cassandraTokenKey, t));
                        applier.apply(metadata);
                    } catch (Exception e) {
                        applier.fail(Status.UNAUTHENTICATED.withCause(e));
                    }
                });
    }

    @Override
    public void thisUsesUnstableApi() {}
}
