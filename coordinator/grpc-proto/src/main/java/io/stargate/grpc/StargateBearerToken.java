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
package io.stargate.grpc;

import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;

public class StargateBearerToken extends CallCredentials {
  public static final Metadata.Key<String> TOKEN_KEY =
      Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

  private final String token;

  public StargateBearerToken(String token) {
    this.token = token;
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(
        () -> {
          try {
            Metadata metadata = new Metadata();
            metadata.put(TOKEN_KEY, token);
            applier.apply(metadata);
          } catch (Exception e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
  }

  @Override
  public void thisUsesUnstableApi() {}
}
