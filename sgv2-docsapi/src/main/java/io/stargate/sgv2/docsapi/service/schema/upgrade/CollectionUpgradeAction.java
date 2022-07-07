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

package io.stargate.sgv2.docsapi.service.schema.upgrade;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.api.exception.ErrorCode;
import io.stargate.sgv2.docsapi.api.v2.namespaces.collections.model.dto.CollectionUpgradeType;

/** Interface for collection upgrade action. */
public interface CollectionUpgradeAction {

  /** @return Upgrade type this action is performing. */
  CollectionUpgradeType getType();

  /**
   * Can this action be performed on the given table.
   *
   * @param collectionTable Collection table schema
   * @return Returns Uni emitting <code>true</code> if this upgrade action can be done. Emits <code>
   *     false</code> otherwise.
   */
  boolean canUpgrade(Schema.CqlTable collectionTable);

  /**
   * Executes the upgrade on the given namespace and collection.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Upgrade can not be performed for given table, with {@link
   *       ErrorCode#DOCS_API_GENERAL_UPGRADE_INVALID}
   * </ol>
   *
   * @param collectionTable Collection table schema
   * @param namespace Target namespace
   * @param collection Target collection
   * @return Uni emitting <code>null</code> if success, or failure if upgrade failed.
   */
  Uni<Void> executeUpgrade(Schema.CqlTable collectionTable, String namespace, String collection);
}
