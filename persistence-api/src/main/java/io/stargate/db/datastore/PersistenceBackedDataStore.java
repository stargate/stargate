package io.stargate.db.datastore;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.stargate.db.Batch;
import io.stargate.db.BatchType;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Statement;
import io.stargate.db.datastore.PersistenceBackedPreparedStatement.PreparedInfo;
import io.stargate.db.datastore.PreparedStatement.Bound;
import io.stargate.db.schema.Schema;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.transport.ProtocolException;

class PersistenceBackedDataStore implements DataStore {
  private final Persistence.Connection connection;
  private final Parameters parameters;

  PersistenceBackedDataStore(Persistence.Connection connection, Parameters parameters) {
    this.connection = connection;
    this.parameters = parameters;
  }

  static ProtocolVersion toDriverVersion(
      org.apache.cassandra.stargate.transport.ProtocolVersion version) {
    switch (version) {
      case V1: // fallthrough on purpose
      case V2:
        // This should likely be rejected much sooner but ...
        throw new ProtocolException("Unsupported protocol version: " + version);
      case V3:
        return ProtocolVersion.V3;
      case V4:
        return ProtocolVersion.V4;
      case V5:
        return ProtocolVersion.V5;
      default:
        throw new AssertionError("Unhandled protocol version: " + version);
    }
  }

  @Override
  public CompletableFuture<ResultSet> query(
      String queryString, UnaryOperator<Parameters> parametersModifier, Object... values) {
    return prepare(queryString).thenCompose(p -> p.execute(parametersModifier, values));
  }

  @Override
  public CompletableFuture<PreparedStatement> prepare(String queryString) {
    return connection
        .prepare(queryString, parameters)
        .thenApply(
            prepared ->
                new PersistenceBackedPreparedStatement(
                    connection, parameters, new PreparedInfo(prepared), queryString));
  }

  @Override
  public CompletableFuture<ResultSet> batch(
      List<Bound> statements, BatchType batchType, UnaryOperator<Parameters> parametersModifier) {
    long queryStartNanos = System.nanoTime();
    Parameters executeParameters = parametersModifier.apply(parameters);
    List<Statement> persistenceStatements =
        statements.stream()
            .map(b -> b.toPersistenceStatement(executeParameters.protocolVersion()))
            .collect(Collectors.toList());
    return connection
        .batch(new Batch(batchType, persistenceStatements), executeParameters, queryStartNanos)
        .thenApply(r -> PersistenceBackedResultSet.create(connection, r, null, executeParameters));
  }

  private Persistence persistence() {
    return connection.persistence();
  }

  @Override
  public Schema schema() {
    return persistence().schema();
  }

  @Override
  public boolean isInSchemaAgreement() {
    return persistence().isInSchemaAgreement();
  }

  @Override
  public void waitForSchemaAgreement() {
    persistence().waitForSchemaAgreement();
  }

  @Override
  public String toString() {
    return String.format("DataStore[connection=%s, parameters=%s]", connection, parameters);
  }
}
