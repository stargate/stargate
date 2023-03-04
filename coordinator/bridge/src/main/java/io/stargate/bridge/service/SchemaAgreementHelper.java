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
package io.stargate.bridge.service;

import io.grpc.Status;
import io.stargate.db.Persistence;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class SchemaAgreementHelper {

  private final Persistence.Connection connection;
  private final int maxRetries;
  private final ScheduledExecutorService executor;

  SchemaAgreementHelper(
      Persistence.Connection connection, int maxRetries, ScheduledExecutorService executor) {
    this.connection = connection;
    this.maxRetries = maxRetries;
    this.executor = executor;
  }

  CompletionStage<Void> waitForAgreement() {
    CompletableFuture<Void> agreementFuture = new CompletableFuture<>();
    waitForAgreement(maxRetries, agreementFuture);
    return agreementFuture;
  }

  private void waitForAgreement(int remainingAttempts, CompletableFuture<Void> agreementFuture) {
    if (connection.isInSchemaAgreement()) {
      agreementFuture.complete(null);
      return;
    }
    if (remainingAttempts <= 1) {
      agreementFuture.completeExceptionally(
          Status.DEADLINE_EXCEEDED
              .withDescription(
                  "Failed to reach schema agreement after " + (200 * maxRetries) + " milliseconds.")
              .asException());
      return;
    }
    executor.schedule(
        () -> waitForAgreement(remainingAttempts - 1, agreementFuture), 200, TimeUnit.MILLISECONDS);
  }
}
