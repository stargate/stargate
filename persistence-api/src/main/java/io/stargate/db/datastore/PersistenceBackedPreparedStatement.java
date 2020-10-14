package io.stargate.db.datastore;

import static java.lang.String.format;

import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistenceBackedPreparedStatement implements PreparedStatement {
  private static final Logger logger =
      LoggerFactory.getLogger(PersistenceBackedPreparedStatement.class);

  private final Persistence.Connection connection;
  private final Parameters parameters;
  private volatile PreparedInfo info;
  private final String queryString;
  private final ByteBuffer unset;

  PersistenceBackedPreparedStatement(
      Persistence.Connection connection,
      Parameters parameters,
      PreparedInfo info,
      String queryString) {
    this.connection = connection;
    this.parameters = parameters;
    this.info = info;
    this.queryString = queryString;
    this.unset = connection.persistence().unsetValue();
  }

  @Override
  public String preparedQueryString() {
    return queryString;
  }

  @Override
  public Bound bind(Object... values) {
    return new PersistenceBackedBound(values);
  }

  private static RuntimeException invalid(String format, Object... args) {
    return new IllegalArgumentException(format(format, args));
  }

  @Override
  public String toString() {
    return format("Prepared[%s]", queryString);
  }

  static class PreparedInfo {
    private final MD5Digest id;
    private final List<Column> bindMarkerDefinitions;

    PreparedInfo(Result.Prepared prepared) {
      this.id = prepared.statementId;
      this.bindMarkerDefinitions = prepared.metadata.columns;
    }
  }

  private class PersistenceBackedBound implements Bound {
    private final Object[] values;

    private PersistenceBackedBound(Object[] values) {
      this.values = values;
    }

    @Override
    public PreparedStatement preparedStatement() {
      return PersistenceBackedPreparedStatement.this;
    }

    @Override
    public List<Object> values() {
      return Collections.unmodifiableList(Arrays.asList(values));
    }

    @Override
    public CompletableFuture<ResultSet> execute(UnaryOperator<Parameters> parametersModifier) {
      long queryStartNanos = System.nanoTime();
      Parameters executeParameters = parametersModifier.apply(parameters);
      CompletableFuture<ResultSet> future = new CompletableFuture<>();
      executeWithRetry(executeParameters, queryStartNanos, future);
      return future;
    }

    @Override
    public BoundStatement toPersistenceStatement(ProtocolVersion protocolVersion) {
      // Avoids races between execute().
      PreparedInfo info = PersistenceBackedPreparedStatement.this.info;
      List<ByteBuffer> boundValues = serializeBoundValues(values, info, protocolVersion);
      return new BoundStatement(info.id, boundValues, null);
    }

    private void executeWithRetry(
        Parameters executeParameters, long queryStartNanos, CompletableFuture<ResultSet> future) {
      doExecute(
          executeParameters,
          queryStartNanos,
          future,
          ex -> {
            if (ex instanceof PreparedQueryNotFoundException) {
              // This could happen due to a schema change between the statement preparation and now,
              // as some schema change can invalidate preparation.
              rePrepareAndRetry(executeParameters, queryStartNanos, future);
            } else {
              future.completeExceptionally(ex);
            }
          });
    }

    private void doExecute(
        Parameters executeParameters,
        long queryStartNanos,
        CompletableFuture<ResultSet> successFuture,
        Consumer<Throwable> onException) {
      BoundStatement statement = toPersistenceStatement(executeParameters.protocolVersion());

      connection
          .execute(statement, executeParameters, queryStartNanos)
          .thenAccept(
              r ->
                  successFuture.complete(
                      PersistenceBackedResultSet.create(
                          connection, r, statement, executeParameters)))
          .exceptionally(
              ex -> {
                onException.accept(ex);
                return null;
              });
    }

    private void rePrepareAndRetry(
        Parameters executeParameters, long queryStartNanos, CompletableFuture<ResultSet> future) {

      logger.debug(
          "Prepared statement (id={}) was invalid when executed. This can happen due to a "
              + "conflicting schema change. Will re-prepare and retry.",
          info.id);
      connection
          .prepare(queryString, parameters)
          .thenAccept(
              prepared -> {
                PersistenceBackedPreparedStatement.this.info = new PreparedInfo(prepared);
                executeWithRetry(executeParameters, queryStartNanos, future);
              })
          .exceptionally(
              ex -> {
                future.completeExceptionally(ex);
                return null;
              });
    }

    private List<ByteBuffer> serializeBoundValues(
        Object[] values, PreparedInfo info, ProtocolVersion protocolVersion) {
      if (info.bindMarkerDefinitions.size() != values.length) {
        throw invalid(
            "Unexpected number of values provided: the prepared statement has %d markers "
                + "but %d values provided",
            info.bindMarkerDefinitions.size(), values.length);
      }

      com.datastax.oss.driver.api.core.ProtocolVersion driverProtocolVersion =
          PersistenceBackedDataStore.toDriverVersion(protocolVersion);
      List<ByteBuffer> serializedValues = new ArrayList<>(values.length);
      for (int i = 0; i < values.length; i++) {
        Column marker = info.bindMarkerDefinitions.get(i);
        Object value = values[i];

        ByteBuffer serialized;
        if (value == null) {
          serialized = null;
        } else if (value.equals(DataStore.UNSET) || value.equals(unset)) {
          serialized = unset;
        } else {
          value = validateValue(marker.name(), marker.type(), value, i);
          ColumnType type = marker.type();
          assert type != null;
          serialized = type.codec().encode(value, driverProtocolVersion);
        }
        serializedValues.add(serialized);
      }
      return serializedValues;
    }

    private Object validateValue(String name, ColumnType type, Object value, int position) {
      try {
        // For collections, we manually apply our ColumnType#validate method to the sub-elements so
        // that the potential coercions that can happen as part of that validation extend inside
        // collections.
        if (type.isList()) {
          if (!(value instanceof List)) {
            throw invalid(
                "For value %d bound to %s, expected a list but got a %s (%s)",
                position, name, value.getClass().getSimpleName(), value);
          }
          ColumnType elementType = type.parameters().get(0);
          List<?> list = (List<?>) value;
          List<Object> validated = new ArrayList<>(list.size());
          for (Object e : list) {
            validated.add(elementType.validate(e, name));
          }
          return validated;
        }
        if (type.isSet()) {
          if (!(value instanceof Set)) {
            throw invalid(
                "For value %d bound to %s, expected a set but got a %s (%s)",
                position, name, value.getClass().getSimpleName(), value);
          }
          ColumnType elementType = type.parameters().get(0);
          Set<?> set = (Set<?>) value;
          Set<Object> validated = new HashSet<>();
          for (Object e : set) {
            validated.add(elementType.validate(e, name));
          }
          return validated;
        }
        if (type.isMap()) {
          if (!(value instanceof Map)) {
            throw invalid(
                "For value %d bound to %s, expected a map but got a %s (%s)",
                position, name, value.getClass().getSimpleName(), value);
          }
          ColumnType keyType = type.parameters().get(0);
          ColumnType valueType = type.parameters().get(1);
          Map<?, ?> map = (Map<?, ?>) value;
          Map<Object, Object> validated = new HashMap<>();
          for (Map.Entry<?, ?> e : map.entrySet()) {
            validated.put(
                keyType.validate(e.getKey(), format("key of map %s", name)),
                valueType.validate(
                    e.getValue(), format("value of map %s for key %s", name, e.getKey())));
          }
          return validated;
        }
        return type.validate(value, name);
      } catch (Column.ValidationException e) {
        throw invalid(
            "Wrong value provided for column '%s'. Provided type '%s' is not compatible with "
                + "expected CQL type '%s'.%s",
            e.location(), e.providedType(), e.expectedCqlType(), e.errorDetails());
      }
    }

    @Override
    public String toString() {
      return format("%s with values %s", preparedStatement(), Arrays.toString(values));
    }
  }
}
