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
package io.stargate.sgv2.docsapi.service.schema;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.function.Function;

/**
 * A version of the {@link CollectionManager} that ensures authorized schema reads are performed on
 * each op.
 *
 * <p>You can auto-wire this bean using: <code>
 * @Inject @Authorized CollectionManager collectionManager.</code>
 */
@ApplicationScoped
@Authorized
public class AuthorizedCollectionManager extends CollectionManager {

  /**
   * {@inheritDoc}
   *
   * <p>Uses {@link SchemaManager#getTableAuthorized(String, String, Function)}
   */
  @Override
  protected Uni<Schema.CqlTable> getTable(String keyspaceName, String tableName) {
    return schemaManager.getTableAuthorized(keyspaceName, tableName, getMissingKeyspaceFailure());
  }

  /**
   * {@inheritDoc}
   *
   * <p>Uses {@link SchemaManager#getTablesAuthorized(String, Function)}
   */
  @Override
  protected Multi<Schema.CqlTable> getAllTables(String keyspaceName) {
    return schemaManager.getTablesAuthorized(keyspaceName, getMissingKeyspaceFailure());
  }
}
