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
package io.stargate.sgv2.common.schema;

import io.stargate.proto.Schema.CqlKeyspaceDescribe;

/**
 * Allows clients to register with {@link SchemaCache} to get notified when keyspaces change.
 *
 * <p>Note no guarantee is made as to which threads will invoke those methods; implementations
 * should use proper synchronization if needed, and not block.
 */
public interface SchemaListener {

  void onCreateKeyspace(CqlKeyspaceDescribe newKeyspace);

  void onUpdateKeyspace(CqlKeyspaceDescribe oldKeyspace, CqlKeyspaceDescribe newKeyspace);

  void onDropKeyspace(CqlKeyspaceDescribe oldKeyspace);
}
