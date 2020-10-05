package io.stargate.db.dse.impl;

import com.google.common.base.Strings;
import io.reactivex.Single;
import io.stargate.db.BatchType;
import io.stargate.db.ClientState;
import io.stargate.db.QueryOptions;
import io.stargate.db.QueryState;
import io.stargate.db.Result;
import io.stargate.db.datastore.DataStore;
import io.stargate.db.dse.datastore.DataStoreUtil;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions.PagingOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.stargate.cql3.functions.FunctionName;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.apache.cassandra.stargate.exceptions.ConfigurationException;
import org.apache.cassandra.stargate.exceptions.FunctionExecutionException;
import org.apache.cassandra.stargate.exceptions.InvalidRequestException;
import org.apache.cassandra.stargate.exceptions.IsBootstrappingException;
import org.apache.cassandra.stargate.exceptions.OperationExecutionException;
import org.apache.cassandra.stargate.exceptions.OverloadedException;
import org.apache.cassandra.stargate.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.stargate.exceptions.ReadFailureException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.RequestFailureReason;
import org.apache.cassandra.stargate.exceptions.SyntaxException;
import org.apache.cassandra.stargate.exceptions.TruncateException;
import org.apache.cassandra.stargate.exceptions.UnauthorizedException;
import org.apache.cassandra.stargate.exceptions.UnavailableException;
import org.apache.cassandra.stargate.exceptions.WriteFailureException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.apache.cassandra.stargate.locator.InetAddressAndPort;
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.ServerError;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.Flags;

public class Conversion {

  // A number of constructors for classes related to QueryOptions but that are not accessible in C*
  // at the moment and need to be accessed through reflection.

  // SpecificOptions(QueryOptions.PagingOptions, ConsistencyLevel serialConsistency, long
  // timestamp, String keyspace)
  private static final Constructor<?> specificOptionsCtor;
  // DefaultQueryOptions(ConsistencyLevel consistency, List<ByteBuffer> values, boolean
  // skipMetadata, QueryOptions.SpecificOptions options, ProtocolVersion protocolVersion)
  private static final Constructor<?> defaultOptionsCtor;
  // OptionsWithNames(QueryOptions.DefaultQueryOptions wrapped, List<String> names)
  private static final Constructor<?> optionsWithNameCtor;

  static {
    try {
      Class<?> defaultOptionsClass =
          Class.forName("org.apache.cassandra.cql3.QueryOptions$DefaultQueryOptions");
      Class<?> specificOptionsClass =
          Class.forName("org.apache.cassandra.cql3.QueryOptions$SpecificOptions");
      Class<?> withNamesClass =
          Class.forName("org.apache.cassandra.cql3.QueryOptions$OptionsWithNames");

      specificOptionsCtor =
          specificOptionsClass.getDeclaredConstructor(
              PagingOptions.class,
              org.apache.cassandra.db.ConsistencyLevel.class,
              long.class,
              String.class);
      specificOptionsCtor.setAccessible(true);

      defaultOptionsCtor =
          defaultOptionsClass.getDeclaredConstructor(
              org.apache.cassandra.db.ConsistencyLevel.class,
              List.class,
              boolean.class,
              specificOptionsClass,
              org.apache.cassandra.transport.ProtocolVersion.class);
      defaultOptionsCtor.setAccessible(true);

      optionsWithNameCtor = withNamesClass.getDeclaredConstructor(defaultOptionsClass, List.class);
      optionsWithNameCtor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(
          "Error during initialization of the persistence layer: some "
              + "reflection-based accesses cannot be setup.",
          e);
    }
  }

  public static org.apache.cassandra.service.QueryState toInternal(
      QueryState<org.apache.cassandra.service.QueryState> state) {
    if (state == null) {
      return org.apache.cassandra.service.QueryState.forInternalCalls();
    }
    return state.getWrapped();
  }

  public static org.apache.cassandra.cql3.QueryOptions toInternal(QueryOptions options) {
    if (options == null) {
      return org.apache.cassandra.cql3.QueryOptions.DEFAULT;
    }
    org.apache.cassandra.transport.ProtocolVersion protocolVersion =
        toInternal(options.getProtocolVersion());
    // Note that PagingState.deserialize below modifies its input, so we duplicate to avoid nasty
    // surprises down the line
    ByteBuffer pagingState =
        options.getPagingState() == null ? null : options.getPagingState().duplicate();
    return createOptions(
        toInternal(options.getConsistency()),
        options.getValues(),
        options.getNames(),
        options.skipMetadata(),
        options.getPageSize(),
        PagingState.deserialize(pagingState, protocolVersion),
        toInternal(options.getSerialConsistency()),
        protocolVersion,
        options.getTimestamp(),
        options.getKeyspace());
  }

  private static org.apache.cassandra.cql3.QueryOptions createOptions(
      org.apache.cassandra.db.ConsistencyLevel consistencyLevel,
      List<ByteBuffer> values,
      List<String> boundNames,
      boolean skipMetadata,
      int pageSize,
      PagingState pagingState,
      org.apache.cassandra.db.ConsistencyLevel serialConsistency,
      org.apache.cassandra.transport.ProtocolVersion protocolVersion,
      long timestamp,
      String keyspace) {
    org.apache.cassandra.cql3.QueryOptions options;
    try {
      PagingOptions pagingOptions = null;
      if (pageSize > 0) {
        pagingOptions =
            new PagingOptions(
                PageSize.rowsSize(pageSize),
                PagingOptions.Mechanism.SINGLE,
                pagingState == null ? null : pagingState.serialize(protocolVersion));
      }

      Object specificOptions =
          specificOptionsCtor.newInstance(pagingOptions, serialConsistency, timestamp, keyspace);
      Object defaultOptions =
          defaultOptionsCtor.newInstance(
              consistencyLevel, values, skipMetadata, specificOptions, protocolVersion);
      options = (org.apache.cassandra.cql3.QueryOptions) defaultOptions;

      // Adds names if there is some.
      if (boundNames != null) {
        options =
            (org.apache.cassandra.cql3.QueryOptions)
                optionsWithNameCtor.newInstance(defaultOptions, boundNames);
      }
    } catch (Exception e) {
      // We can't afford to ignore that: the values wouldn't be in the proper order, and worst
      // case scenario, this could end up inserting values in the wrong columns, which essentially
      // boils down to corrupting the use DB.
      throw new RuntimeException(
          "Unexpected error while trying to bind the query values by name", e);
    }
    return options;
  }

  private static PagingOptions buildPagingOptions(QueryOptions options) {
    PageSize size =
        options.getPageSize() > 0 ? PageSize.rowsSize(options.getPageSize()) : PageSize.NULL;
    return new PagingOptions(size, PagingOptions.Mechanism.SINGLE, options.getPagingState());
  }

  public static org.apache.cassandra.service.ClientState toInternal(
      ClientState<org.apache.cassandra.service.ClientState> state) {
    if (state == null) {
      return org.apache.cassandra.service.ClientState.forInternalCalls();
    }
    return state.getWrapped();
  }

  public static org.apache.cassandra.db.ConsistencyLevel toInternal(ConsistencyLevel cl) {
    return cl == null ? null : org.apache.cassandra.db.ConsistencyLevel.fromCode(cl.code);
  }

  public static ConsistencyLevel toExternal(org.apache.cassandra.db.ConsistencyLevel cl) {
    return cl == null ? null : ConsistencyLevel.fromCode(cl.code);
  }

  public static org.apache.cassandra.transport.ProtocolVersion toInternal(
      ProtocolVersion protocolVersion) {
    // TODO(mpenick): Allow older version?
    return protocolVersion == null
        ? null
        : org.apache.cassandra.transport.ProtocolVersion.decode(protocolVersion.asInt());
  }

  public static ProtocolVersion toExternal(
      org.apache.cassandra.transport.ProtocolVersion protocolVersion) {
    return protocolVersion == null ? null : ProtocolVersion.decode(protocolVersion.asInt(), true);
  }

  public static InetAddressAndPort toExternal(InetAddress internal) {
    return InetAddressAndPort.getByAddress(internal);
  }

  public static InetAddressAndPort toInternal(InetAddressAndPort external) {
    return InetAddressAndPort.getByAddressOverrideDefaults(external.address, external.port);
  }

  public static org.apache.cassandra.utils.MD5Digest toInternal(MD5Digest id) {
    return org.apache.cassandra.utils.MD5Digest.wrap(id.bytes);
  }

  public static MD5Digest toExternal(org.apache.cassandra.utils.MD5Digest id) {
    return MD5Digest.wrap(id.bytes);
  }

  public static Map<InetAddressAndPort, RequestFailureReason> toExternal(
      Map<InetAddress, org.apache.cassandra.exceptions.RequestFailureReason> internal) {
    Map<InetAddressAndPort, RequestFailureReason> external = new HashMap<>(internal.size());
    for (Map.Entry<InetAddress, org.apache.cassandra.exceptions.RequestFailureReason> entry :
        internal.entrySet()) {
      external.put(
          InetAddressAndPort.getByAddress(entry.getKey()),
          RequestFailureReason.fromCode(entry.getValue().codeForNativeProtocol()));
    }
    return external;
  }

  public static WriteType toExternal(org.apache.cassandra.db.WriteType internal) {
    return WriteType.fromOrdinal(internal.ordinal());
  }

  public static FunctionName toExternal(org.apache.cassandra.cql3.functions.FunctionName internal) {
    return new FunctionName(internal.keyspace, internal.name);
  }

  public static RuntimeException toExternal(CassandraException e) {
    switch (e.code()) {
      case SERVER_ERROR:
        return new ServerError(e.getMessage());
      case BAD_CREDENTIALS:
        return new AuthenticationException(e.getMessage(), e.getCause());
      case UNAVAILABLE:
        org.apache.cassandra.exceptions.UnavailableException ue =
            (org.apache.cassandra.exceptions.UnavailableException) e;
        return new UnavailableException(
            ue.getMessage(), toExternal(ue.consistency), ue.required, ue.alive);
      case OVERLOADED:
        return new OverloadedException(e.getMessage());
      case IS_BOOTSTRAPPING:
        return new IsBootstrappingException();
      case TRUNCATE_ERROR:
        return e.getCause() == null
            ? new TruncateException(e.getMessage())
            : new TruncateException(e.getCause());
      case WRITE_TIMEOUT:
        org.apache.cassandra.exceptions.WriteTimeoutException wte =
            (org.apache.cassandra.exceptions.WriteTimeoutException) e;
        return new WriteTimeoutException(
            toExternal(wte.writeType),
            toExternal(wte.consistency),
            wte.received,
            wte.blockFor,
            wte.getMessage());
      case READ_TIMEOUT:
        org.apache.cassandra.exceptions.ReadTimeoutException rte =
            (org.apache.cassandra.exceptions.ReadTimeoutException) e;
        return new ReadTimeoutException(
            toExternal(rte.consistency), rte.received, rte.blockFor, rte.dataPresent);
      case READ_FAILURE:
        org.apache.cassandra.exceptions.ReadFailureException rfe =
            (org.apache.cassandra.exceptions.ReadFailureException) e;
        return new ReadFailureException(
            toExternal(rfe.consistency),
            rfe.received,
            rfe.blockFor,
            rfe.dataPresent,
            toExternal(rfe.failureReasonByEndpoint));
      case FUNCTION_FAILURE:
        if (e instanceof org.apache.cassandra.exceptions.FunctionExecutionException) {
          org.apache.cassandra.exceptions.FunctionExecutionException fee =
              (org.apache.cassandra.exceptions.FunctionExecutionException) e;
          return new FunctionExecutionException(
              toExternal(fee.functionName), fee.argTypes, fee.detail);
        } else if (e instanceof org.apache.cassandra.exceptions.OperationExecutionException) {
          return new OperationExecutionException(e.getMessage());
        }
        return e;
      case WRITE_FAILURE:
        org.apache.cassandra.exceptions.WriteFailureException wfe =
            (org.apache.cassandra.exceptions.WriteFailureException) e;
        return new WriteFailureException(
            toExternal(wfe.consistency),
            wfe.received,
            wfe.blockFor,
            toExternal(wfe.writeType),
            toExternal(wfe.failureReasonByEndpoint));
      case SYNTAX_ERROR:
        return new SyntaxException(e.getMessage());
      case UNAUTHORIZED:
        return new UnauthorizedException(e.getMessage(), e.getCause());
      case INVALID:
        return new InvalidRequestException(e.getMessage());
      case CONFIG_ERROR:
        return new ConfigurationException(e.getMessage(), e.getCause());
      case ALREADY_EXISTS:
        org.apache.cassandra.exceptions.AlreadyExistsException aee =
            (org.apache.cassandra.exceptions.AlreadyExistsException) e;
        return Strings.isNullOrEmpty(aee.cfName)
            ? new AlreadyExistsException(aee.ksName)
            : new AlreadyExistsException(aee.ksName, aee.cfName);
      case UNPREPARED:
        org.apache.cassandra.exceptions.PreparedQueryNotFoundException pnfe =
            (org.apache.cassandra.exceptions.PreparedQueryNotFoundException) e;
        return new PreparedQueryNotFoundException(MD5Digest.wrap(pnfe.id.bytes));
    }
    return e;
  }

  public static Result.ResultMetadata toResultMetadata(
      org.apache.cassandra.cql3.ResultSet.ResultMetadata metadata,
      org.apache.cassandra.transport.ProtocolVersion version) {
    List<Column> columns = new ArrayList<>();
    if (metadata.names != null) {
      metadata.names.forEach(
          c ->
              columns.add(
                  ImmutableColumn.builder()
                      .keyspace(c.ksName)
                      .table(c.cfName)
                      .name(c.name.toString())
                      .type(DataStoreUtil.getTypeFromInternal(c.type))
                      .build()));
    }

    EnumSet<Result.Flag> flags = EnumSet.noneOf(Result.Flag.class);

    if (Flags.contains(metadata.getFlags(), ResultSet.ResultSetFlag.GLOBAL_TABLES_SPEC))
      flags.add(Result.Flag.fromId(ResultSet.ResultSetFlag.GLOBAL_TABLES_SPEC));

    if (Flags.contains(metadata.getFlags(), ResultSet.ResultSetFlag.HAS_MORE_PAGES))
      flags.add(Result.Flag.fromId(ResultSet.ResultSetFlag.HAS_MORE_PAGES));

    if (Flags.contains(metadata.getFlags(), ResultSet.ResultSetFlag.METADATA_CHANGED))
      flags.add(Result.Flag.fromId(ResultSet.ResultSetFlag.METADATA_CHANGED));

    ByteBuffer pagingState = null;
    if (version != null) {
      pagingState =
          metadata.getPagingResult() == null || metadata.getPagingResult().state == null
              ? null
              : metadata.getPagingResult().state;
    }

    MD5Digest resultMetadataId =
        metadata.getResultMetadataId() == null
            ? null
            : Conversion.toExternal(metadata.getResultMetadataId());
    return new Result.ResultMetadata(flags, columns, resultMetadataId, pagingState);
  }

  public static Result.PreparedMetadata toPreparedMetadata(
      List<ColumnSpecification> names, short[] indexes) {
    List<Column> columns = new ArrayList<>();
    names.forEach(
        c ->
            columns.add(
                ImmutableColumn.builder()
                    .keyspace(c.ksName)
                    .table(c.cfName)
                    .name(c.name.toString())
                    .type(DataStoreUtil.getTypeFromInternal(c.type))
                    .build()));

    EnumSet<Result.Flag> flags = EnumSet.noneOf(Result.Flag.class);
    if (!names.isEmpty() && ColumnSpecification.allInSameTable(names))
      flags.add(Result.Flag.GLOBAL_TABLES_SPEC);

    return new Result.PreparedMetadata(flags, columns, indexes);
  }

  public static Result.SchemaChangeMetadata toSchemaChangeMetadata(ResultMessage resultMessage) {
    ResultMessage.SchemaChange schemaChange = (ResultMessage.SchemaChange) resultMessage;
    Event.SchemaChange change = schemaChange.change;
    return new Result.SchemaChangeMetadata(
        change.change.toString(),
        change.target.toString(),
        change.keyspace,
        change.name,
        change.argTypes);
  }

  public static Result toResult(
      ResultMessage resultMessage, org.apache.cassandra.transport.ProtocolVersion version) {
    switch (resultMessage.kind) {
      case VOID:
        return Result.VOID;
      case ROWS:
        return new Result.Rows(
            ((ResultMessage.Rows) resultMessage).result.rows,
            toResultMetadata(((ResultMessage.Rows) resultMessage).result.metadata, version));
      case SET_KEYSPACE:
        return new Result.SetKeyspace(((ResultMessage.SetKeyspace) resultMessage).keyspace);
      case SCHEMA_CHANGE:
        return new Result.SchemaChange(toSchemaChangeMetadata(resultMessage));
    }

    throw new ProtocolException("Unexpected type for RESULT message");
  }

  public static Result.Prepared toPrepared(ResultMessage.Prepared prepared) {
    QueryHandler.Prepared preparedStatement =
        QueryProcessor.instance.getPrepared(prepared.statementId);
    return new Result.Prepared(
        Conversion.toExternal(prepared.statementId),
        Conversion.toExternal(prepared.resultMetadataId),
        toResultMetadata(prepared.resultMetadata, null),
        toPreparedMetadata(
            prepared.metadata.names,
            preparedStatement.statement.getPartitionKeyBindVariableIndexes()));
  }

  public static Throwable handleException(Throwable t) {
    if (t instanceof org.apache.cassandra.exceptions.UnauthorizedException) {
      return DataStore.UnauthorizedException.rbac(t);
    }

    if (t instanceof CassandraException) {
      return Conversion.toExternal((CassandraException) t);
    }

    if (t instanceof org.apache.cassandra.transport.ProtocolException) {
      // Note that ProtocolException is not a CassandraException
      org.apache.cassandra.transport.ProtocolException ex =
          (org.apache.cassandra.transport.ProtocolException) t;
      return new ProtocolException(t.getMessage(), toExternal(ex.getForcedProtocolVersion()));
    }

    return t;
  }

  public static <U> CompletableFuture<U> toFuture(Single<U> single) {
    CompletableFuture<U> future = new CompletableFuture<>();
    single.subscribe(future::complete, future::completeExceptionally);
    return future;
  }

  public static List<Object> toInternalQueryOrIds(List<Object> queryOrIds) {
    List<Object> internal = new ArrayList<>(queryOrIds.size());
    for (Object o : queryOrIds) {
      if (o instanceof MD5Digest) {
        internal.add(toInternal((MD5Digest) o));
      } else {
        internal.add(o);
      }
    }
    return internal;
  }

  public static BatchStatement.Type toInternal(BatchType external) {
    switch (external) {
      case LOGGED:
        return BatchStatement.Type.LOGGED;
      case UNLOGGED:
        return BatchStatement.Type.UNLOGGED;
      case COUNTER:
        return BatchStatement.Type.COUNTER;
    }
    throw new IllegalArgumentException("Invalid batch type");
  }
}
