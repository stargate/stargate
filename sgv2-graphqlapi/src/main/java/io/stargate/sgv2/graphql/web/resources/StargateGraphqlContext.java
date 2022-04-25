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
package io.stargate.sgv2.graphql.web.resources;

import com.google.common.base.MoreObjects;
import io.stargate.bridge.proto.QueryOuterClass.BatchParameters;
import io.stargate.bridge.proto.QueryOuterClass.BatchQuery;
import io.stargate.bridge.proto.QueryOuterClass.Query;
import io.stargate.bridge.proto.QueryOuterClass.QueryParameters;
import io.stargate.bridge.proto.QueryOuterClass.Response;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.graphql.schema.CassandraFetcher;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;

public class StargateGraphqlContext {

  private final HttpServletRequest httpRequest;
  private final StargateBridgeClient bridge;
  private final GraphqlCache graphqlCache;
  private final BatchContext batchContext = new BatchContext();

  private volatile boolean overloaded;

  public StargateGraphqlContext(
      HttpServletRequest httpRequest, StargateBridgeClient bridge, GraphqlCache graphqlCache) {
    this.httpRequest = httpRequest;
    this.bridge = bridge;
    this.graphqlCache = graphqlCache;
  }

  public StargateBridgeClient getBridge() {
    return bridge;
  }

  public BatchContext getBatchContext() {
    return batchContext;
  }

  /**
   * Records the fact that at least one CQL query in the current execution failed with an OVERLOADED
   * error. This will be translated into an HTTP 429 error at the resource layer.
   */
  public void setOverloaded() {
    this.overloaded = true;
  }

  public boolean isOverloaded() {
    return overloaded;
  }

  /**
   * Encapsulates logic to add multiple queries contained in the same operation that need to be
   * executed in a batch.
   */
  public static class BatchContext {
    private final List<BatchQuery> queries = new ArrayList<>();
    private int operationCount;
    private final CompletableFuture<Response> executionFuture = new CompletableFuture<>();
    private final AtomicReference<QueryParameters> parameters = new AtomicReference<>();

    public CompletableFuture<Response> getExecutionFuture() {
      return executionFuture;
    }

    public synchronized List<BatchQuery> getQueries() {
      return queries;
    }

    public void setExecutionResult(CompletionStage<Response> response) {
      response
          .thenApply(executionFuture::complete)
          .exceptionally(executionFuture::completeExceptionally);
    }

    public void setExecutionResult(Exception ex) {
      executionFuture.completeExceptionally(ex);
    }

    public synchronized int add(Query query) {
      queries.add(toBatchQuery(query));
      operationCount += 1;
      return operationCount;
    }

    public synchronized int add(List<Query> newQueries) {
      newQueries.forEach(query -> queries.add(toBatchQuery(query)));
      operationCount += 1;
      return operationCount;
    }

    /**
     * Sets the parameters to use for the batch.
     *
     * @return whether the update succeeded (either the parameters weren't set, or the were already
     *     set but to the same values)
     */
    public boolean setParameters(QueryParameters newParameters) {
      while (true) {
        QueryParameters currentParameters = this.parameters.get();
        if (currentParameters == null) {
          // try to set, but if we race we need to loop to get and compare the new value
          if (parameters.compareAndSet(null, newParameters)) {
            return true;
          }
        } else {
          return newParameters.equals(currentParameters);
        }
      }
    }

    public BatchParameters getParameters() {
      QueryParameters queryParameters =
          MoreObjects.firstNonNull(parameters.get(), CassandraFetcher.DEFAULT_PARAMETERS);
      return toBatchParameters(queryParameters);
    }

    // gRPC uses different types for regular or batched queries/parameters. We use the regular
    // variants in common fetcher code, but need to do the conversion here.
    private BatchQuery toBatchQuery(Query query) {
      return BatchQuery.newBuilder().setCql(query.getCql()).setValues(query.getValues()).build();
    }

    private BatchParameters toBatchParameters(QueryParameters queryParameters) {
      return BatchParameters.newBuilder()
          .setConsistency(queryParameters.getConsistency())
          .setTracing(queryParameters.getTracing())
          .setTimestamp(queryParameters.getTimestamp())
          .setSerialConsistency(queryParameters.getSerialConsistency())
          .setNowInSeconds(queryParameters.getNowInSeconds())
          .setTracingConsistency(queryParameters.getTracingConsistency())
          .setSkipMetadata(queryParameters.getSkipMetadata())
          .build();
    }
  }
}
