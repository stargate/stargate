package io.stargate.db.datastore;

import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.PersistenceBackedPreparedStatement.PreparedInfo;
import io.stargate.db.schema.Schema;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

class PersistenceBackedDataStore implements DataStore {
  private final Persistence.Connection connection;
  private final Parameters parameters;

  PersistenceBackedDataStore(Persistence.Connection connection, Parameters parameters) {
    this.connection = connection;
    this.parameters = parameters;
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
