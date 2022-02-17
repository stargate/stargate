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
package io.stargate.sgv2.common.grpc;

/**
 * Allows external components to get notified when {@link StargateBridgeSchema} invalidates a
 * keyspace from its internal cache.
 *
 * <p>This is a signal that if the external component is holding on to a copy of the keyspace
 * metadata, it should fetch it again.
 *
 * @see StargateBridgeSchema#register(KeyspaceInvalidationListener)
 */
public interface KeyspaceInvalidationListener {

  /**
   * @param keyspaceName note that in a multi-tenant environment, this is the full "decorated"
   *     keyspace name.
   */
  void onKeyspaceInvalidated(String keyspaceName);
}
