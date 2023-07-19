package io.stargate.db.datastore;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.Batch;
import io.stargate.db.BatchType;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.SimpleStatement;
import io.stargate.db.Statement;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.Query;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Schema;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceBackedDataStore implements DataStore {
  private static final Logger logger = LoggerFactory.getLogger(PersistenceBackedDataStore.class);

  private final Persistence.Connection connection;
  private final DataStoreOptions options;
  private final Codec valueCodec;

  public PersistenceBackedDataStore(Persistence.Connection connection, DataStoreOptions options) {
    this.connection = connection;
    this.options = options;
    this.valueCodec = new Codec(parameters().protocolVersion(), connection.persistence());
  }

  private Parameters parameters() {
    return options.defaultParameters();
  }

  @Override
  public Codec valueCodec() {
    return valueCodec;
  }

  private Statement toPersistenceStatement(BoundQuery query) {
    List<TypedValue> values = query.values();
    List<ByteBuffer> buffers = new ArrayList<>(values.size());
    for (TypedValue value : values) {
      buffers.add(value.bytes());
    }
    Query<?> boundedQuery = query.source().query();
    return boundedQuery.preparedId().isPresent()
        ? new BoundStatement(boundedQuery.preparedId().get(), buffers, null)
        : new SimpleStatement(boundedQuery.queryStringForPreparation(), buffers);
  }

  private void validateExecuteParameters(Parameters executeParameters) {
    if (parameters() == executeParameters) {
      return;
    }

    Preconditions.checkArgument(
        !executeParameters.skipMetadataInResult(),
        "Invalid execution parameters: you should not set 'skipMetadataInResult', this"
            + "is handled internally by DataStore.");
    Preconditions.checkArgument(
        executeParameters.protocolVersion() == parameters().protocolVersion(),
        "Invalid execution parameters: cannot modify the protocol version for execution "
            + "(the DataStore version %s != %s, the execution parameters version).",
        parameters().protocolVersion(),
        executeParameters.protocolVersion());
  }

  @Override
  public <B extends BoundQuery> CompletableFuture<Query<B>> prepare(Query<B> query) {
    String queryToPrepare = query.queryStringForPreparation();

    // check first in the connection cache
    return Optional.ofNullable(connection.getPrepared(queryToPrepare, parameters()))

        // if so finalize
        .map(CompletableFuture::completedFuture)

        // otherwise, go and prepare
        .orElseGet(() -> connection.prepare(queryToPrepare, parameters()))

        // then apply transformation to the BoundQuery
        .thenApply(prepared -> query.withPreparedId(prepared.statementId));
  }

  @Override
  public CompletableFuture<ResultSet> execute(
      BoundQuery query, UnaryOperator<Parameters> parametersModifier) {
    long queryStartNanos = System.nanoTime();
    Parameters executeParameters = parametersModifier.apply(parameters());
    validateExecuteParameters(executeParameters);

    CompletableFuture<ResultSet> future = new CompletableFuture<>();
    if (query.source().query().preparedId().isPresent()) {
      executeWithRetry(query, executeParameters, queryStartNanos, future);
    } else if (options.alwaysPrepareQueries()) {
      prepareAndRetry(query.source(), executeParameters, queryStartNanos, future);
    } else {
      doExecute(query, executeParameters, queryStartNanos, future, future::completeExceptionally);
    }
    return future;
  }

  private void executeWithRetry(
      BoundQuery query,
      Parameters executeParameters,
      long queryStartNanos,
      CompletableFuture<ResultSet> future) {
    doExecute(
        query,
        executeParameters,
        queryStartNanos,
        future,
        ex -> {
          if (ex instanceof PreparedQueryNotFoundException) {
            // This could happen due to a schema change between the statement preparation and now,
            // as some schema change can invalidate preparation.
            logger.debug(
                "Prepared statement (id={}) was invalid when executed. This can happen due to a "
                    + "conflicting schema change. Will re-prepare and retry.",
                ((PreparedQueryNotFoundException) ex).id);
            prepareAndRetry(query.source(), executeParameters, queryStartNanos, future);
          } else {
            future.completeExceptionally(ex);
          }
        });
  }

  private void doExecute(
      BoundQuery query,
      Parameters executeParameters,
      long queryStartNanos,
      CompletableFuture<ResultSet> successFuture,
      Consumer<Throwable> onException) {
    Statement statement = toPersistenceStatement(query);
    connection
        .execute(statement, executeParameters, queryStartNanos)
        .thenAccept(
            r ->
                successFuture.complete(
                    PersistenceBackedResultSet.create(connection, r, statement, executeParameters)))
        .exceptionally(
            ex -> {
              onException.accept(ex);
              return null;
            });
  }

  private void prepareAndRetry(
      BoundQuery.Source<?> bound,
      Parameters executeParameters,
      long queryStartNanos,
      CompletableFuture<ResultSet> future) {
    prepare(bound.query())
        .thenAccept(
            prepared ->
                executeWithRetry(
                    prepared.bindValues(bound.values()),
                    executeParameters,
                    queryStartNanos,
                    future))
        .exceptionally(
            ex -> {
              future.completeExceptionally(ex);
              return null;
            });
  }

  @Override
  public CompletableFuture<ResultSet> batch(
      Collection<BoundQuery> queries,
      BatchType batchType,
      UnaryOperator<Parameters> parametersModifier) {
    long queryStartNanos = System.nanoTime();
    Parameters executeParameters = parametersModifier.apply(parameters());
    validateExecuteParameters(executeParameters);
    List<Statement> persistenceStatements =
        queries.stream().map(this::toPersistenceStatement).collect(Collectors.toList());
    return batch(persistenceStatements, batchType, executeParameters, queryStartNanos);
  }

  private CompletableFuture<ResultSet> batch(
      List<Statement> statements,
      BatchType batchType,
      Parameters executeParameters,
      long queryStartNanos) {

    return connection
        .batch(new Batch(batchType, statements), executeParameters, queryStartNanos)
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
    return connection.isInSchemaAgreement();
  }

  @Override
  public boolean supportsSecondaryIndex() {
    return persistence().supportsSecondaryIndex();
  }

  @Override
  public boolean supportsSAI() {
    return persistence().supportsSAI();
  }

  @Override
  public boolean supportsVectorSearch() {
    return persistence().supportsVectorSearch();
  }

  @Override
  public boolean supportsLoggedBatches() {
    return persistence().supportsLoggedBatches();
  }

  @Override
  public void waitForSchemaAgreement() {
    connection.waitForSchemaAgreement();
  }

  @Override
  public String toString() {
    return String.format("DataStore[connection=%s, options=%s]", connection, options);
  }
}
