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
package io.stargate.it.storage;

/**
 * Provides data for connecting to storage clusters managed by {@link ExternalStorage}. Instances of
 * this interface are automatically injected into test constructor and method parameters with
 * compatible declared type.
 */
public interface ClusterConnectionInfo {
  String id();

  String seedAddress();

  int storagePort();

  int cqlPort();

  String clusterVersion();

  boolean isDse();

  String clusterName();

  String datacenter();

  String rack();

  /** Indicates whether the cluster supports CQL COUNTERs. */
  default boolean supportsCounters() {
    return true;
  }

  default boolean supportsSAI() {
    // OSS C* does not support SAI (yet?)
    return isDse();
  }
}
