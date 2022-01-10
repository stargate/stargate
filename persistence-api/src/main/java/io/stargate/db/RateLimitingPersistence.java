package io.stargate.db;

import io.stargate.db.Result.Prepared;
import io.stargate.db.limiter.RateLimitingDecision;
import io.stargate.db.limiter.RateLimitingManager;
import io.stargate.db.schema.Schema;
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Persistence} wrapper that delegates all methods to its wrapped persistence, but may
 * apply some rate limiting on queries before doing so.
 *
 * <p>The actual decision on when to rate limiting and how much is delegated to the concrete {@link
 * RateLimitingManager} and this is class just does some fairly thin wiring because said manager and
 * the underlying persistence.
 */
public class RateLimitingPersistence implements Persistence {
  private static final Logger logger = LoggerFactory.getLogger(DbActivator.class);

  private final Persistence persistence;
  private final RateLimitingManager manager;

  public RateLimitingPersistence(Persistence persistence, RateLimitingManager manager) {
    this.persistence = persistence;
    this.manager = manager;
    logger.info("Enabling rate limiting: {}", manager.description());
  }

  @Override
  public String name() {
    return persistence.name();
  }

  @Override
  public Schema schema() {
    return persistence.schema();
  }

  @Override
  public void registerEventListener(EventListener listener) {
    persistence.registerEventListener(listener);
  }

  @Override
  public void unregisterEventListener(EventListener listener) {
    persistence.unregisterEventListener(listener);
  }

  @Override
  public Authenticator getAuthenticator() {
    return persistence.getAuthenticator();
  }

  @Override
  public void setRpcReady(boolean status) {
    persistence.setRpcReady(status);
  }

  @Override
  public Connection newConnection(ClientInfo clientInfo) {
    return new RateLimitedConnection(
        persistence.newConnection(clientInfo), manager.forNewConnection(clientInfo));
  }

  @Override
  public Connection newConnection() {
    return new RateLimitedConnection(persistence.newConnection(), manager.forNewConnection());
  }

  @Override
  public ByteBuffer unsetValue() {
    return persistence.unsetValue();
  }

  @Override
  public boolean isInSchemaAgreement() {
    return persistence.isInSchemaAgreement();
  }

  @Override
  public boolean isInSchemaAgreementWithStorage() {
    return persistence.isInSchemaAgreementWithStorage();
  }

  @Override
  public boolean isSchemaAgreementAchievable() {
    return persistence.isSchemaAgreementAchievable();
  }

  @Override
  public boolean supportsSecondaryIndex() {
    return persistence.supportsSecondaryIndex();
  }

  @Override
  public boolean supportsSAI() {
    return persistence.supportsSAI();
  }

  @Override
  public Map<String, List<String>> cqlSupportedOptions() {
    return persistence.cqlSupportedOptions();
  }

  @Override
  public void executeAuthResponse(Runnable handler) {
    persistence.executeAuthResponse(handler);
  }

  private class RateLimitedConnection implements Connection {
    private final Connection connection;
    private final RateLimitingManager.ConnectionManager rateLimiter;

    private RateLimitedConnection(
        Connection connection, RateLimitingManager.ConnectionManager rateLimiter) {
      this.connection = connection;
      this.rateLimiter = rateLimiter;
    }

    @Override
    public Persistence persistence() {
      return RateLimitingPersistence.this;
    }

    @Override
    public void login(AuthenticatedUser user) throws AuthenticationException {
      connection.login(user);
      rateLimiter.onUserLogged(user);
    }

    @Override
    public Optional<AuthenticatedUser> loggedUser() {
      return connection.loggedUser();
    }

    @Override
    public Optional<ClientInfo> clientInfo() {
      return connection.clientInfo();
    }

    @Override
    public Optional<String> usedKeyspace() {
      return connection.usedKeyspace();
    }

    @Override
    public CompletableFuture<Prepared> prepare(String query, Parameters parameters) {
      RateLimitingDecision decision = rateLimiter.forPrepare(query, parameters);
      return decision.apply(() -> connection.prepare(query, parameters));
    }

    @Override
    public CompletableFuture<Result> execute(
        Statement statement, Parameters parameters, long queryStartNanoTime) {
      RateLimitingDecision decision = rateLimiter.forExecute(statement, parameters);
      return decision.apply(() -> connection.execute(statement, parameters, queryStartNanoTime));
    }

    @Override
    public CompletableFuture<Result> batch(
        Batch batch, Parameters parameters, long queryStartNanoTime) {
      RateLimitingDecision decision = rateLimiter.forBatch(batch, parameters);
      return decision.apply(() -> connection.batch(batch, parameters, queryStartNanoTime));
    }

    @Override
    public void setCustomProperties(Map<String, String> customProperties) {
      connection.setCustomProperties(customProperties);
    }

    @Override
    public ByteBuffer makePagingState(PagingPosition position, Parameters parameters) {
      return connection.makePagingState(position, parameters);
    }

    @Override
    public RowDecorator makeRowDecorator(TableName table) {
      return connection.makeRowDecorator(table);
    }
  }
}
