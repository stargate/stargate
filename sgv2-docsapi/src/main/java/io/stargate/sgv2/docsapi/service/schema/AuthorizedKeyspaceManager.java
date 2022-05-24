package io.stargate.sgv2.docsapi.service.schema;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.docsapi.service.schema.qualifier.Authorized;
import javax.enterprise.context.ApplicationScoped;

/**
 * A version of the {@link KeyspaceManager} that ensures authorized schema reads are performed on
 * each op.
 *
 * <p>You can auto-wire this bean using: <code>@Inject @Authorized KeyspaceManager keyspaceManager.
 * </code>
 */
@ApplicationScoped
@Authorized
public class AuthorizedKeyspaceManager extends KeyspaceManager {

  /**
   * {@inheritDoc}
   *
   * <p>Uses {@link
   * io.stargate.sgv2.docsapi.service.schema.common.SchemaManager#getKeyspaceAuthorized(String)}
   */
  @Override
  protected Uni<Schema.CqlKeyspaceDescribe> getKeyspace(String keyspaceName) {
    return schemaManager.getKeyspaceAuthorized(keyspaceName);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Uses {@link
   * io.stargate.sgv2.docsapi.service.schema.common.SchemaManager#getKeyspacesAuthorized()}
   */
  @Override
  protected Multi<Schema.CqlKeyspaceDescribe> getKeyspaces() {
    return schemaManager.getKeyspacesAuthorized();
  }
}
