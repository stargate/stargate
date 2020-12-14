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
package io.stargate.graphql.web;

import io.stargate.db.datastore.DataStore;
import io.stargate.db.datastore.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletRequest;

public class HttpAwareContext {

  private HttpServletRequest request;

  // We need to manually maintain state between multiple selections in a single mutation
  // operation to execute them as a batch.
  // Currently graphql-java batching support is only restricted to queries and not mutations
  // See https://www.graphql-java.com/documentation/v15/batching/ and
  // https://github.com/graphql-java/graphql-java/blob/v15.0/src/main/java/graphql/execution/instrumentation/dataloader/DataLoaderDispatcherInstrumentation.java#L112-L115
  // For more information.
  private final BatchContext batchContext = new BatchContext();

  public HttpAwareContext(HttpServletRequest request) {
    this.request = request;
  }

  public String getAuthToken() {
    return request.getHeader("X-Cassandra-Token");
  }

  public BatchContext getBatchContext() {
    return batchContext;
  }

  /**
   * Encapsulates logic to add multiple statements contained in the same operation that need to be
   * executed in a batch.
   */
  public static class BatchContext {
    private final List<String> statements = new ArrayList<>();
    private final CompletableFuture<ResultSet> executionFuture = new CompletableFuture<>();
    private AtomicReference<DataStore> dataStore = new AtomicReference<>();

    public CompletableFuture<ResultSet> getExecutionFuture() {
      return executionFuture;
    }

    public synchronized List<String> getStatements() {
      return statements;
    }

    public void setExecutionResult(CompletableFuture<ResultSet> result) {
      result.whenComplete(
          (r, e) -> {
            if (e != null) {
              executionFuture.completeExceptionally(e);
            } else {
              executionFuture.complete(r);
            }
          });
    }

    public void setExecutionResult(Exception ex) {
      executionFuture.completeExceptionally(ex);
    }

    public synchronized int add(String query) {
      statements.add(query);
      return statements.size();
    }

    /** Sets the data store and returns whether it was already set */
    public boolean setDataStore(DataStore dataStore) {
      return this.dataStore.getAndSet(dataStore) != null;
    }

    public Optional<DataStore> getDataStore() {
      return Optional.ofNullable(this.dataStore.get());
    }
  }
}
