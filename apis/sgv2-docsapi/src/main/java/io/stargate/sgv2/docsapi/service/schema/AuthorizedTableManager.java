package io.stargate.sgv2.docsapi.service.schema;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import java.util.function.Function;
import javax.enterprise.context.ApplicationScoped;

/**
 * A version of the {@link TableManager} that ensures authorized schema reads are performed on each
 * op.
 *
 * <p>You can auto-wire this bean using: <code>@Inject @Authorized TableManager tableManager.</code>
 */
@ApplicationScoped
@Authorized
public class AuthorizedTableManager extends TableManager {

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
