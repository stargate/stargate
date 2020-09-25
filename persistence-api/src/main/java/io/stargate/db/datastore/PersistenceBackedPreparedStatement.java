package io.stargate.db.datastore;

import static java.lang.String.format;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import io.stargate.db.BoundStatement;
import io.stargate.db.Parameters;
import io.stargate.db.Persistence;
import io.stargate.db.Result;
import io.stargate.db.datastore.query.Parameter;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PersistenceBackedPreparedStatement implements PreparedStatement {
  private static final Logger logger =
      LoggerFactory.getLogger(PersistenceBackedPreparedStatement.class);

  private final Persistence.Connection connection;
  private final Parameters parameters;
  private volatile PreparedInfo info;
  private final ProtocolVersion driverProtocolVersion;
  private final String queryString;

  PersistenceBackedPreparedStatement(
      Persistence.Connection connection,
      Parameters parameters,
      PreparedInfo info,
      String queryString) {
    this.connection = connection;
    this.parameters = parameters;
    this.info = info;
    this.queryString = queryString;
    this.driverProtocolVersion = toDriverVersion(parameters.protocolVersion());
  }

  private static ProtocolVersion toDriverVersion(
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
  public CompletableFuture<ResultSet> execute(
      Optional<ConsistencyLevel> consistencyLevel, Object... values) {
    long queryStartNanos = System.nanoTime();
    Parameters executeParameters =
        consistencyLevel.isPresent()
            ? parameters.withConsistencyLevel(consistencyLevel.get())
            : parameters;

    CompletableFuture<ResultSet> future = new CompletableFuture<>();
    executeWithRetry(values, executeParameters, queryStartNanos, future);
    return future;
  }

  private void executeWithRetry(
      Object[] values,
      Parameters executeParameters,
      long queryStartNanos,
      CompletableFuture<ResultSet> future) {
    doExecute(
        values,
        executeParameters,
        queryStartNanos,
        future,
        ex -> {
          if (ex instanceof PreparedQueryNotFoundException) {
            // This could happen due to a schema change between the statement preparation and now,
            // as some schema change can invalidate preparation.
            rePrepareAndRetry(values, executeParameters, queryStartNanos, future);
          } else {
            future.completeExceptionally(ex);
          }
        });
  }

  private void doExecute(
      Object[] values,
      Parameters executeParameters,
      long queryStartNanos,
      CompletableFuture<ResultSet> successFuture,
      Consumer<Throwable> onException) {
    PreparedInfo info = this.info; // Avoiding races between execute().
    List<ByteBuffer> boundValues = serializeBoundValues(values, info);
    BoundStatement statement = new BoundStatement(info.id, boundValues, null);

    connection
        .execute(statement, executeParameters, queryStartNanos)
        .thenAccept(r -> successFuture.complete(createResultSet(r, statement, executeParameters)))
        .exceptionally(
            ex -> {
              onException.accept(ex);
              return null;
            });
  }

  private void rePrepareAndRetry(
      Object[] values,
      Parameters executeParameters,
      long queryStartNanos,
      CompletableFuture<ResultSet> future) {

    logger.debug(
        "Prepared statement (id={}) was invalid when executed. This can happen due to a "
            + "conflicting schema change. Will re-prepare and retry.",
        info.id);
    connection
        .prepare(queryString, parameters)
        .thenAccept(
            prepared -> {
              this.info = new PreparedInfo(prepared);
              executeWithRetry(values, executeParameters, queryStartNanos, future);
            })
        .exceptionally(
            ex -> {
              future.completeExceptionally(ex);
              return null;
            });
  }

  private ResultSet createResultSet(
      Result result, BoundStatement statement, Parameters executeParameters) {
    switch (result.kind) {
      case Prepared:
        throw new AssertionError(
            "Shouldn't get a 'Prepared' result when executing a prepared statement");
      case SchemaChange:
        connection.persistence().waitForSchemaAgreement();
        return ResultSet.empty(true);
      case Void: // fallthrough on purpose
      case SetKeyspace:
        return ResultSet.empty();
      case Rows:
        return new PersistenceBackedResultSet(
            connection, executeParameters, statement, driverProtocolVersion, (Result.Rows) result);
      default:
        throw new AssertionError("Unhandled result type: " + result.kind);
    }
  }

  private static InvalidRequestException invalid(String format, Object... args) {
    return new InvalidRequestException(format(format, args));
  }

  private List<ByteBuffer> serializeBoundValues(Object[] values, PreparedInfo info) {
    if (info.bindMarkerDefinitions.size() != values.length) {
      throw invalid(
          "Unexpected number of values provided: the prepared statement has %d markers "
              + "but %d values provided",
          info.bindMarkerDefinitions.size(), values.length);
    }

    List<ByteBuffer> serializedValues = new ArrayList<>(values.length);
    for (int i = 0; i < values.length; i++) {
      Column marker = info.bindMarkerDefinitions.get(i);
      Object value = values[i];

      ByteBuffer serialized;
      if (value == null) {
        serialized = null;
      } else if (value.equals(Parameter.UNSET)) {
        serialized = connection.persistence().unsetValue();
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
          "Wrong value provided for %s. Provided type '%s' is not compatible with "
              + "expected CQL type '%s'.%s",
          e.location(), e.providedType(), e.expectedCqlType(), e.errorDetails());
    }
  }

  static class PreparedInfo {
    final MD5Digest id;
    final List<Column> bindMarkerDefinitions;

    PreparedInfo(Result.Prepared prepared) {
      this.id = prepared.statementId;
      this.bindMarkerDefinitions = prepared.metadata.columns;
    }
  }
}
