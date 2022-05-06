package io.stargate.sgv2.docsapi.service.schema;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
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

  @Override
  protected Uni<Schema.CqlTable> getTable(String keyspaceName, String tableName) {
    return schemaManager.getTableAuthorized(
        keyspaceName, tableName, getMissingKeyspaceFailure(keyspaceName));
  }
}
