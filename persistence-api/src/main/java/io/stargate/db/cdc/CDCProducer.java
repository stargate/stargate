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
package io.stargate.db.cdc;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.MutationEvent;

/** Represents a publisher of mutations events for Change Data Capture (CDC). */
public interface CDCProducer extends AutoCloseable {
  /**
   * Initializes the {@link CDCProducer}. It will be invoked once in the lifetime of the instance
   * before any calls to send.
   *
   * @param options The passthrough options.
   */
  CompletableFuture<Void> init(Map<String, Object> options);

  /**
   * Publishes the mutation event. It will only be invoked for mutations that should be tracked by
   * the CDC.
   */
  CompletableFuture<Void> publish(MutationEvent mutation);
}
