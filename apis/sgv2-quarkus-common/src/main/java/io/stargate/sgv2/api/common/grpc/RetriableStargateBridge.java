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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.api.common.grpc;

import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.config.GrpcConfig;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;

/**
 * An implementation of the {@link StargateBridge} that executes retries based on the
 * GrpcConfig.Retries configuration.
 */
public class RetriableStargateBridge implements StargateBridge {

  private final StargateBridge delegate;

  private final GrpcRetryPredicate predicate;

  private final boolean enabled;

  private final int maxAttempts;

  public RetriableStargateBridge(
      StargateBridge delegate, GrpcRetryPredicate predicate, GrpcConfig grpcConfig) {
    this.delegate = delegate;
    this.predicate = predicate;
    this.enabled = grpcConfig.retries().enabled();
    this.maxAttempts = grpcConfig.retries().maxAttempts();
  }

  @Override
  public Uni<QueryOuterClass.Response> executeQuery(QueryOuterClass.Query request) {
    return withRetries(delegate.executeQuery(request));
  }

  @Override
  public Uni<Schema.QueryWithSchemaResponse> executeQueryWithSchema(
      Schema.QueryWithSchema request) {
    return withRetries(delegate.executeQueryWithSchema(request));
  }

  @Override
  public Uni<QueryOuterClass.Response> executeBatch(QueryOuterClass.Batch request) {
    return withRetries(delegate.executeBatch(request));
  }

  @Override
  public Uni<Schema.CqlKeyspaceDescribe> describeKeyspace(Schema.DescribeKeyspaceQuery request) {
    return withRetries(delegate.describeKeyspace(request));
  }

  @Override
  public Uni<Schema.AuthorizeSchemaReadsResponse> authorizeSchemaReads(
      Schema.AuthorizeSchemaReadsRequest request) {
    return withRetries(delegate.authorizeSchemaReads(request));
  }

  @Override
  public Uni<Schema.SupportedFeaturesResponse> getSupportedFeatures(
      Schema.SupportedFeaturesRequest request) {
    return withRetries(delegate.getSupportedFeatures(request));
  }

  private <T> Uni<T> withRetries(Uni<T> source) {
    // if disabled do nothing
    if (!enabled) {
      return source;
    }

    // otherwise wrap in retry
    return source
        .onFailure(
            t -> {
              if (t instanceof StatusRuntimeException sre) {
                return predicate.test(sre);
              }
              return false;
            })
        .retry()
        .atMost(maxAttempts);
  }
}
