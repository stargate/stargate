package io.stargate.db.datastore;

import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.datastore.PersistenceBackedPreparedStatement.PreparedInfo;
import io.stargate.db.schema.Index;
import io.stargate.db.schema.Schema;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.stargate.db.ConsistencyLevel;

class PersistenceBackedDataStore implements DataStore {
  private final Persistence.Connection connection;
  private final Parameters parameters;

  PersistenceBackedDataStore(Persistence.Connection connection, Parameters parameters) {
    this.connection = connection;
    this.parameters = parameters;
  }

  @Override
  public CompletableFuture<ResultSet> query(
      String cql, Optional<ConsistencyLevel> consistencyLevel, Object... values) {
    return prepare(cql, Optional.empty()).thenCompose(p -> p.execute(consistencyLevel, values));
  }

  @Override
  public CompletableFuture<PreparedStatement> prepare(String cql, Optional<Index> index) {
    return connection
        .prepare(cql, parameters)
        .thenApply(
            prepared ->
                new PersistenceBackedPreparedStatement(
                    connection, parameters, new PreparedInfo(prepared), cql));
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
