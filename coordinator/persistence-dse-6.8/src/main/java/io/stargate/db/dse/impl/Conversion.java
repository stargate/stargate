package io.stargate.db.dse.impl;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.stargate.db.BatchType;
import io.stargate.db.PagingPosition;
import io.stargate.db.Parameters;
import io.stargate.db.Result;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.ImmutableUserDefinedType;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryOptions.PagingOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.DynamicCompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LineStringType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.PointType;
import org.apache.cassandra.db.marshal.PolygonType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.ArrayBackedRow;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PagingState.RowMark;
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
import org.apache.cassandra.stargate.exceptions.PersistenceException;
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
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Conversion {
  private static final Logger logger = LoggerFactory.getLogger(Conversion.class);
  private static final NoSpamLogger noSpamLogger =
      NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);

  // This LivenessInfo is used only for constructing intermediate `Row` objects so as to build
  // a `PagingState`, but it does not actually go into the `PagingState` object
  private static final LivenessInfo DUMMY_LIVENESS_INFO = LivenessInfo.create(0, 0);

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

  private static final Map<Class<? extends AbstractType>, Column.Type> TYPE_MAPPINGS;

  static {
    Map<Class<? extends AbstractType>, Column.Type> types = new HashMap<>();
    // standard types
    types.put(AsciiType.instance.getClass(), Column.Type.Ascii);
    types.put(LongType.instance.getClass(), Column.Type.Bigint);
    types.put(BytesType.instance.getClass(), Column.Type.Blob);
    types.put(ByteType.instance.getClass(), Column.Type.Tinyint);
    types.put(BooleanType.instance.getClass(), Column.Type.Boolean);
    types.put(CompositeType.class, Column.Type.Composite);
    types.put(CounterColumnType.instance.getClass(), Column.Type.Counter);
    types.put(SimpleDateType.instance.getClass(), Column.Type.Date);
    types.put(DecimalType.instance.getClass(), Column.Type.Decimal);
    types.put(DoubleType.instance.getClass(), Column.Type.Double);
    types.put(DurationType.instance.getClass(), Column.Type.Duration);
    types.put(DynamicCompositeType.class, Column.Type.DynamicComposite);
    types.put(FloatType.instance.getClass(), Column.Type.Float);
    types.put(InetAddressType.instance.getClass(), Column.Type.Inet);
    types.put(Int32Type.instance.getClass(), Column.Type.Int);
    types.put(IntegerType.instance.getClass(), Column.Type.Varint);
    types.put(ShortType.instance.getClass(), Column.Type.Smallint);
    types.put(UUIDType.instance.getClass(), Column.Type.Uuid);
    types.put(UTF8Type.instance.getClass(), Column.Type.Text);
    types.put(TimeType.instance.getClass(), Column.Type.Time);
    types.put(TimestampType.instance.getClass(), Column.Type.Timestamp);
    types.put(TimeUUIDType.instance.getClass(), Column.Type.Timeuuid);

    // extra for DSE
    types.put(PointType.class, Column.Type.Point);
    types.put(PolygonType.class, Column.Type.Polygon);
    types.put(LineStringType.class, Column.Type.LineString);

    TYPE_MAPPINGS = ImmutableMap.copyOf(types);
  }

  public static ByteBuffer toPagingState(
      TableMetadata table, PagingPosition pos, Parameters parameters) {
    Object[] pkValues =
        table.partitionKeyColumns().stream()
            .map(c -> pos.requiredValue(c.name.toCQLString()))
            .toArray();
    Clustering clustering = table.partitionKeyAsClusteringComparator().make(pkValues);
    ByteBuffer serializedKey = clustering.serializeAsPartitionKey();

    org.apache.cassandra.transport.ProtocolVersion protocolVersion =
        toInternal(parameters.protocolVersion());

    RowMark rowMark;
    switch (pos.resumeFrom()) {
      case NEXT_PARTITION:
        rowMark = null;
        break;

      case NEXT_ROW:
        ByteBuffer[] ccValues =
            table.clusteringColumns().stream()
                .map(c -> pos.requiredValue(c.name.toCQLString()))
                .toArray(ByteBuffer[]::new);

        Clustering rowClustering = Clustering.make(ccValues);
        ArrayBackedRow row = ArrayBackedRow.noCellLiveRow(rowClustering, DUMMY_LIVENESS_INFO);
        rowMark = RowMark.create(table, row, protocolVersion);
        break;

      default:
        throw new UnsupportedOperationException("Unsupported paging mode: " + pos.resumeFrom());
    }

    PagingState pagingState =
        new PagingState(
            serializedKey, rowMark, pos.remainingRows(), pos.remainingRowsInPartition(), false);

    return pagingState.serialize(protocolVersion);
  }

  public static QueryOptions toInternal(
      List<ByteBuffer> values, List<String> boundNames, Parameters parameters) {
    org.apache.cassandra.transport.ProtocolVersion protocolVersion =
        toInternal(parameters.protocolVersion());
    // Note that PagingState.deserialize below modifies its input, so we duplicate to avoid nasty
    // surprises down the line
    PagingState pagingState =
        parameters
            .pagingState()
            .map(s -> PagingState.deserialize(s.duplicate(), protocolVersion))
            .orElse(null);
    return createOptions(
        toInternal(parameters.consistencyLevel()),
        values,
        boundNames,
        parameters.skipMetadataInResult(),
        parameters.pageSize().orElse(-1),
        pagingState,
        toInternal(parameters.serialConsistencyLevel().orElse(ConsistencyLevel.SERIAL)),
        protocolVersion,
        parameters.defaultTimestamp().orElse(Long.MIN_VALUE),
        parameters.defaultKeyspace().orElse(null));
  }

  private static QueryOptions createOptions(
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
    QueryOptions options;
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
      options = (QueryOptions) defaultOptions;

      // Adds names if there is some.
      if (boundNames != null) {
        options = (QueryOptions) optionsWithNameCtor.newInstance(defaultOptions, boundNames);
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
      external.put(InetAddressAndPort.getByAddress(entry.getKey()), toExternal(entry.getValue()));
    }
    return external;
  }

  private static RequestFailureReason toExternal(
      org.apache.cassandra.exceptions.RequestFailureReason internal) {
    try {
      // Convert to Stargate enum values by name since SG replies on OSS protocol codes
      // for request failure reasons and they do match DSE-specific failure codes.
      return RequestFailureReason.valueOf(internal.name());
    } catch (IllegalArgumentException e) {
      logger.debug("Unknown failure reason: {}", internal);
      // Note: the "UNKNOWN" failure reason itself should match by name. If we ended up here it
      // means the error code is new in DSE and Stargate has not yet been updated to handle it.
      return RequestFailureReason.FROM_THE_FUTURE;
    }
  }

  public static WriteType toExternal(org.apache.cassandra.db.WriteType internal) {
    return WriteType.fromOrdinal(internal.ordinal());
  }

  public static FunctionName toExternal(org.apache.cassandra.cql3.functions.FunctionName internal) {
    return new FunctionName(internal.keyspace, internal.name);
  }

  public static PersistenceException toExternal(CassandraException e) {
    switch (e.code()) {
      case SERVER_ERROR:
        return addSuppressed(new ServerError(e.getMessage()), e);
      case BAD_CREDENTIALS:
        return addSuppressed(new AuthenticationException(e.getMessage(), e.getCause()), e);
      case UNAVAILABLE:
        org.apache.cassandra.exceptions.UnavailableException ue =
            (org.apache.cassandra.exceptions.UnavailableException) e;
        return addSuppressed(
            new UnavailableException(
                ue.getMessage(), toExternal(ue.consistency), ue.required, ue.alive),
            e);
      case OVERLOADED:
        return addSuppressed(new OverloadedException(e.getMessage()), e);
      case IS_BOOTSTRAPPING:
        return addSuppressed(new IsBootstrappingException(), e);
      case TRUNCATE_ERROR:
        return addSuppressed(
            e.getCause() == null
                ? new TruncateException(e.getMessage())
                : new TruncateException(e.getCause()),
            e);
      case WRITE_TIMEOUT:
        org.apache.cassandra.exceptions.WriteTimeoutException wte =
            (org.apache.cassandra.exceptions.WriteTimeoutException) e;
        return addSuppressed(
            new WriteTimeoutException(
                toExternal(wte.writeType),
                toExternal(wte.consistency),
                wte.received,
                wte.blockFor,
                wte.getMessage()),
            e);
      case READ_TIMEOUT:
        org.apache.cassandra.exceptions.ReadTimeoutException rte =
            (org.apache.cassandra.exceptions.ReadTimeoutException) e;
        return addSuppressed(
            new ReadTimeoutException(
                toExternal(rte.consistency), rte.received, rte.blockFor, rte.dataPresent),
            e);
      case READ_FAILURE:
        org.apache.cassandra.exceptions.ReadFailureException rfe =
            (org.apache.cassandra.exceptions.ReadFailureException) e;
        return addSuppressed(
            new ReadFailureException(
                toExternal(rfe.consistency),
                rfe.received,
                rfe.blockFor,
                rfe.dataPresent,
                toExternal(rfe.failureReasonByEndpoint)),
            e);
      case FUNCTION_FAILURE:
        if (e instanceof org.apache.cassandra.exceptions.FunctionExecutionException) {
          org.apache.cassandra.exceptions.FunctionExecutionException fee =
              (org.apache.cassandra.exceptions.FunctionExecutionException) e;
          return addSuppressed(
              new FunctionExecutionException(
                  toExternal(fee.functionName), fee.argTypes, fee.detail),
              e);
        } else if (e instanceof org.apache.cassandra.exceptions.OperationExecutionException) {
          return addSuppressed(new OperationExecutionException(e.getMessage()), e);
        }
        break;
      case WRITE_FAILURE:
        org.apache.cassandra.exceptions.WriteFailureException wfe =
            (org.apache.cassandra.exceptions.WriteFailureException) e;
        return addSuppressed(
            new WriteFailureException(
                toExternal(wfe.consistency),
                wfe.received,
                wfe.blockFor,
                toExternal(wfe.writeType),
                toExternal(wfe.failureReasonByEndpoint)),
            e);
      case SYNTAX_ERROR:
        return addSuppressed(new SyntaxException(e.getMessage()), e);
      case UNAUTHORIZED:
        // Not adding suppressed exception to avoid information leaking
        return new UnauthorizedException(e.getMessage(), e.getCause());
      case INVALID:
        return addSuppressed(new InvalidRequestException(e.getMessage()), e);
      case CONFIG_ERROR:
        return addSuppressed(new ConfigurationException(e.getMessage(), e.getCause()), e);
      case ALREADY_EXISTS:
        org.apache.cassandra.exceptions.AlreadyExistsException aee =
            (org.apache.cassandra.exceptions.AlreadyExistsException) e;
        return addSuppressed(
            Strings.isNullOrEmpty(aee.cfName)
                ? new AlreadyExistsException(aee.ksName)
                : new AlreadyExistsException(aee.ksName, aee.cfName),
            e);
      case UNPREPARED:
        org.apache.cassandra.exceptions.PreparedQueryNotFoundException pnfe =
            (org.apache.cassandra.exceptions.PreparedQueryNotFoundException) e;
        return addSuppressed(new PreparedQueryNotFoundException(MD5Digest.wrap(pnfe.id.bytes)), e);
      case PROTOCOL_ERROR:
      case CLIENT_WRITE_FAILURE:
        // fall through
    }
    noSpamLogger.error(
        "Unhandled Cassandra exception code {} in the persistence conversion "
            + "code. This should be fixed (but a ServerError will be use in the meantime)");
    return new ServerError(e);
  }

  private static PersistenceException addSuppressed(
      PersistenceException e, RuntimeException suppressed) {
    e.addSuppressed(suppressed);
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
                      .type(getTypeFromInternal(c.type))
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
                    .type(getTypeFromInternal(c.type))
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
      ResultMessage resultMessage,
      org.apache.cassandra.transport.ProtocolVersion version,
      @Nullable List<String> warnings) {
    return toResultInternal(resultMessage, version)
        .setTracingId(resultMessage.getTracingId())
        .setWarnings(warnings);
  }

  private static Result toResultInternal(
      ResultMessage resultMessage, org.apache.cassandra.transport.ProtocolVersion version) {
    switch (resultMessage.kind) {
      case VOID:
        return new Result.Void();
      case ROWS:
        return new Result.Rows(
            ((ResultMessage.Rows) resultMessage).result.rows,
            toResultMetadata(((ResultMessage.Rows) resultMessage).result.metadata, version));
      case SET_KEYSPACE:
        return new Result.SetKeyspace(((ResultMessage.SetKeyspace) resultMessage).keyspace);
      case SCHEMA_CHANGE:
        return new Result.SchemaChange(toSchemaChangeMetadata(resultMessage));
      case PREPARED:
        ResultMessage.Prepared prepared = (ResultMessage.Prepared) resultMessage;
        PreparedWithInfo preparedWithInfo = (PreparedWithInfo) prepared;
        return new Result.Prepared(
            Conversion.toExternal(prepared.statementId),
            Conversion.toExternal(prepared.resultMetadataId),
            toResultMetadata(prepared.resultMetadata, null),
            toPreparedMetadata(
                prepared.metadata.names, preparedWithInfo.getPartitionKeyBindVariableIndexes()),
            preparedWithInfo.isIdempotent(),
            preparedWithInfo.isUseKeyspace());
    }

    throw new ProtocolException("Unexpected type for RESULT message: " + resultMessage.kind);
  }

  public static PersistenceException convertInternalException(Throwable t) {
    if (t instanceof CassandraException) {
      return Conversion.toExternal((CassandraException) t);
    }

    if (t instanceof org.apache.cassandra.transport.ProtocolException) {
      // Note that ProtocolException is not a CassandraException
      org.apache.cassandra.transport.ProtocolException ex =
          (org.apache.cassandra.transport.ProtocolException) t;
      return new ProtocolException(t.getMessage(), toExternal(ex.getForcedProtocolVersion()));
    }

    if (t instanceof org.apache.cassandra.transport.ServerError) {
      // Nor is ServerError
      return new ServerError(t.getMessage());
    }

    // We shouldn't get random error so let's log it.
    logger.error("Unexpected error thrown by persistence", t);
    return new ServerError(t);
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

  public static Column.ColumnType getTypeFromInternal(AbstractType abstractType) {
    if (abstractType instanceof ReversedType) {
      return getTypeFromInternal(((ReversedType) abstractType).baseType);
    }
    if (abstractType instanceof MapType) {
      return Column.Type.Map.of(
              getTypeFromInternal(((MapType) abstractType).getKeysType()),
              getTypeFromInternal(((MapType) abstractType).getValuesType()))
          .frozen(!abstractType.isMultiCell());
    } else if (abstractType instanceof SetType) {
      return Column.Type.Set.of(getTypeFromInternal(((SetType) abstractType).getElementsType()))
          .frozen(!abstractType.isMultiCell());
    } else if (abstractType instanceof ListType) {
      return Column.Type.List.of(getTypeFromInternal(((ListType) abstractType).getElementsType()))
          .frozen(!abstractType.isMultiCell());
    } else if (abstractType.getClass().equals(TupleType.class)) {
      TupleType tupleType = ((TupleType) abstractType);
      return Column.Type.Tuple.of(
              ((TupleType) abstractType)
                  .subTypes().stream()
                      .map(t -> getTypeFromInternal(t))
                      .toArray(Column.ColumnType[]::new))
          .frozen(!tupleType.isMultiCell());
    } else if (abstractType.getClass().equals(UserType.class)) {
      UserType udt = (UserType) abstractType;
      return ImmutableUserDefinedType.builder()
          .keyspace(udt.keyspace)
          .name(udt.getNameAsString())
          .addAllColumns(getUDTColumns(udt))
          .build()
          .frozen(!udt.isMultiCell());
    }

    Column.Type type = TYPE_MAPPINGS.get(abstractType.getClass());
    Preconditions.checkArgument(
        type != null, "Unknown type mapping for %s", abstractType.getClass());
    return type;
  }

  public static List<Column> getUDTColumns(UserType userType) {
    List<Column> columns = new ArrayList<>(userType.fieldTypes().size());
    for (int i = 0; i < userType.fieldTypes().size(); i++) {
      columns.add(
          ImmutableColumn.builder()
              .name(userType.fieldName(i).toString())
              .type(getTypeFromInternal(userType.fieldType(i)))
              .kind(Column.Kind.Regular)
              .build());
    }
    return columns;
  }
}
