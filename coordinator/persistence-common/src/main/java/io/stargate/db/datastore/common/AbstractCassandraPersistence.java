package io.stargate.db.datastore.common;

import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import io.stargate.db.AuthenticatedUser;
import io.stargate.db.ClientInfo;
import io.stargate.db.Persistence;
import io.stargate.db.schema.Schema;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience base class aimed at simplifying the writing of a Cassandra-like {@link Persistence}
 * layer.
 *
 * <p>Technically, this is meant to limit as much as reasonable the amount of code duplication
 * between the persistence layers for various C* versions.
 *
 * @param <Config>
 * @param <K> the concrete class for keyspace metadata in the persistence layer.
 * @param <T> the concrete class for table metadata in the persistence layer.
 * @param <C> the concrete class for column metadata in the persistence layer.
 * @param <U> the concrete class for user types in the persistence layer.
 * @param <I> the concrete class for secondary indexes metadata in the persistence layer.
 * @param <V> the concrete class for materialized views metadata in the persistence layer.
 */
public abstract class AbstractCassandraPersistence<Config, K, T, C, U, I, V>
    implements Persistence {

  private static final Logger logger = LoggerFactory.getLogger(AbstractCassandraPersistence.class);

  private final String name;

  private final AbstractCassandraSchemaConverter<K, T, C, U, I, V> schemaConverter;

  // The schema exposed by stargate. It is translated from the internal C* schema during
  // initialization, and then updated every time the internal schema changes through a schema
  // listener callback.
  private volatile Schema schema;

  protected AbstractCassandraPersistence(String name) {
    this.name = name;
    this.schemaConverter = newSchemaConverter();
  }

  /** Creates a new, stateless, converter for the schema of the concrete persistence layer. */
  protected abstract AbstractCassandraSchemaConverter<K, T, C, U, I, V> newSchemaConverter();

  /** The current schema of the concrete persistence layer. */
  protected abstract Iterable<K> currentInternalSchema();

  /**
   * Register an internal schema listener that runs the provided runnable every time the internal
   * schema of the persistence layer changes.
   *
   * <p>This is guaranteed to be called only once for each persistence instance, during
   * initialization. Implementations should usually keep track of the registered listener so they
   * can implement {@link #unregisterInternalSchemaListener()}.
   */
  protected abstract void registerInternalSchemaListener(Runnable onSchemaChange);

  /**
   * Unregister the internal schema listener registered through {@link
   * #registerInternalSchemaListener(Runnable)}, if necessary.
   */
  protected abstract void unregisterInternalSchemaListener();

  /**
   * Actually initialize the persistence layer (basically, the {@link #initialize} implementation,
   * but modulo the parts that are already handled by this abstract facility)
   */
  protected abstract void initializePersistence(Config config);

  /**
   * Actually destroys the persistence layer (basically, the {@link #destroy} implementation, but
   * modulo the parts that are already handled by this abstract facility)
   */
  protected abstract void destroyPersistence();

  @Override
  public final String name() {
    return name;
  }

  @Override
  public final Schema schema() {
    if (schema == null) {
      throw new IllegalStateException(
          String.format(
              "The schema cannot be accessed until the " + "%s persistence layer is initialized",
              name));
    }
    return schema;
  }

  public final void initialize(Config config) {
    logger.info("Initializing {}", name);

    if (!Boolean.parseBoolean(System.getProperty("stargate.developer_mode"))) {
      System.setProperty("cassandra.join_ring", "false");
    }

    initializePersistence(config);

    schema = computeCurrentSchema();
    registerInternalSchemaListener(() -> schema = computeCurrentSchema());
  }

  private Schema computeCurrentSchema() {
    return schemaConverter.convertCassandraSchema(currentInternalSchema());
  }

  public final void destroy() {
    destroyPersistence();
    unregisterInternalSchemaListener();
  }

  @Override
  public String toString() {
    return name();
  }

  protected abstract static class AbstractConnection implements Connection {
    private final @Nullable ClientInfo clientInfo;
    private volatile @Nullable AuthenticatedUser loggedUser;

    protected AbstractConnection(@Nullable ClientInfo clientInfo) {
      this.clientInfo = clientInfo;
    }

    @Override
    public Optional<ClientInfo> clientInfo() {
      return Optional.ofNullable(clientInfo);
    }

    protected abstract void loginInternally(AuthenticatedUser user);

    @Override
    public void login(AuthenticatedUser user) {
      // Note that we do the actual login first, so that if it fails, loggedUser remains null
      loginInternally(user);
      this.loggedUser = user;
    }

    @Override
    public Optional<AuthenticatedUser> loggedUser() {
      return Optional.ofNullable(loggedUser);
    }

    @Override
    public String toString() {
      Map<String, String> params = new LinkedHashMap<>();
      params.put("persistence", persistence().toString());
      if (clientInfo != null) {
        params.put("client", clientInfo.toString());
      }
      if (loggedUser != null) {
        params.put("logged", loggedUser.name());
      }
      return String.format(
          "Connection[%s]", Joiner.on(",").withKeyValueSeparator("=").join(params));
    }
  }
}
