package io.stargate.db.limiter;

import io.stargate.db.AuthenticatedUser;
import io.stargate.db.Batch;
import io.stargate.db.ClientInfo;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Statement;

/**
 * Manages rate limiting.
 *
 * <p>A manager takes decision how which queries should be rate limited, and how.
 */
public interface RateLimitingManager {

  /**
   * A string describing the manager. This is used in logging messages to describe what the manager
   * actually does.
   */
  String description();

  /**
   * Creates a sub-manager dedicated to a newly created {@link Persistence.Connection}.
   *
   * <p>This is called when {@link Persistence#newConnection()} is called, so when no {@link
   * ClientInfo} is provided for the connection.
   */
  ConnectionManager forNewConnection();

  /**
   * Creates a sub-manager dedicated to a newly created {@link Persistence.Connection}.
   *
   * <p>This is called when {@link Persistence#newConnection(ClientInfo)} is called, and the
   * provided {@link ClientInfo} is the one passed as argument to this method.
   */
  ConnectionManager forNewConnection(ClientInfo clientInfo);

  /** Sub-class of the manager dedicated to a particular {@link Persistence.Connection}. */
  interface ConnectionManager {

    /**
     * Called when a use is successfully logged on the connection this manager was created for.
     *
     * @param user the user logged in.
     */
    void onUserLogged(AuthenticatedUser user);

    /**
     * The rate limiting decision for the query consisting of preparing the provided query (on the
     * connection this manager was created for).
     *
     * <p>This is called before an actual {@link Persistence.Connection#prepare} call, and the
     * decision is applied to the call.
     *
     * @param query the query to be prepared.
     * @param parameters the parameters for the preparation.
     * @return the decision for this query preparation.
     */
    RateLimitingDecision forPrepare(String query, Parameters parameters);

    /**
     * The rate limiting decision for the query consisting of executing the provided statement (on
     * the connection this manager was created for).
     *
     * <p>This is called before an actual {@link Persistence.Connection#execute} call, and the
     * decision is applied to the call.
     *
     * @param statement the statement to be executed.
     * @param parameters the parameters for the execution.
     * @return the decision for this statement execution.
     */
    RateLimitingDecision forExecute(Statement statement, Parameters parameters);

    /**
     * The rate limiting decision for the query consisting of executing the provided batch (on the
     * connection this manager was created for).
     *
     * <p>This is called before an actual {@link Persistence.Connection#batch} call, and the
     * decision is applied to the call.
     *
     * @param batch the batch to be executed.
     * @param parameters the parameters for the execution.
     * @return the decision for this batch execution.
     */
    RateLimitingDecision forBatch(Batch batch, Parameters parameters);
  }
}
