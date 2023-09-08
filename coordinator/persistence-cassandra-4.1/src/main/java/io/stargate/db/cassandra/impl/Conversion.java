package io.stargate.db.cassandra.impl;

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
import io.stargate.db.schema.TableName;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
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
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
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
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.exceptions.CassandraException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PagingState.RowMark;
import org.apache.cassandra.stargate.cql3.functions.FunctionName;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.db.WriteType;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;
import org.apache.cassandra.stargate.exceptions.AuthenticationException;
import org.apache.cassandra.stargate.exceptions.CDCWriteException;
import org.apache.cassandra.stargate.exceptions.CasWriteUnknownResultException;
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
import org.apache.cassandra.stargate.transport.ProtocolException;
import org.apache.cassandra.stargate.transport.ProtocolVersion;
import org.apache.cassandra.stargate.transport.ServerError;
import org.apache.cassandra.stargate.utils.MD5Digest;
import org.apache.cassandra.transport.Event;
import org.apache.cassandra.transport.messages.ResultMessage;
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

  private static final Map<Class<? extends AbstractType>, Column.Type> TYPE_MAPPINGS;

  static {
    Map<Class<? extends AbstractType>, Column.Type> types = new HashMap<>();
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

    TYPE_MAPPINGS = ImmutableMap.copyOf(types);
  }

  public static TableMetadata toTableMetadata(TableName tableName) {
    return Schema.instance.validateTable(tableName.keyspace(), tableName.name());
  }

  public static ByteBuffer toPagingState(PagingPosition pos, Parameters parameters) {
    TableMetadata table = toTableMetadata(pos.tableName());

    Object[] pkValues =
        table.partitionKeyColumns().stream()
            .map(c -> pos.requiredValue(c.name.toCQLString()))
            .toArray();
    Clustering<?> clustering = table.partitionKeyAsClusteringComparator().make(pkValues);
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

        Clustering<?> rowClustering = Clustering.make(ccValues);
        BTreeRow row = BTreeRow.noCellLiveRow(rowClustering, DUMMY_LIVENESS_INFO);
        rowMark = RowMark.create(table, row, protocolVersion);
        break;

      default:
        throw new UnsupportedOperationException("Unsupported paging mode: " + pos.resumeFrom());
    }

    PagingState pagingState =
        new PagingState(
            serializedKey, rowMark, pos.remainingRows(), pos.remainingRowsInPartition());

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
    QueryOptions options =
        QueryOptions.create(
            toInternal(parameters.consistencyLevel()),
            values,
            parameters.skipMetadataInResult(),
            parameters.pageSize().orElse(-1),
            pagingState,
            toInternal(parameters.serialConsistencyLevel().orElse(ConsistencyLevel.SERIAL)),
            protocolVersion,
            parameters.defaultKeyspace().orElse(null),
            parameters.defaultTimestamp().orElse(Long.MIN_VALUE),
            parameters.nowInSeconds().orElse(Integer.MIN_VALUE));

    // Adds names if there is some.
    if (boundNames != null && !boundNames.isEmpty()) {
      try {
        options = ReflectionUtils.newOptionsWithNames(options, boundNames);
      } catch (Exception e) {
        // We can't afford to ignore that: the values wouldn't be in the proper order, and worst
        // case scenario, this could end up inserting values in the wrong columns, which essentially
        // boils down to corrupting the use DB.
        throw new RuntimeException(
            "Unexpected error while trying to bind the query values by name", e);
      }
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
        : org.apache.cassandra.transport.ProtocolVersion.decode(protocolVersion.asInt(), true);
  }

  public static ProtocolVersion toExternal(
      org.apache.cassandra.transport.ProtocolVersion protocolVersion) {
    return protocolVersion == null ? null : ProtocolVersion.decode(protocolVersion.asInt(), true);
  }

  public static org.apache.cassandra.stargate.locator.InetAddressAndPort toExternal(
      InetAddressAndPort internal) {
    return org.apache.cassandra.stargate.locator.InetAddressAndPort.getByAddressOverrideDefaults(
        internal.getAddress(), internal.getPort());
  }

  public static InetAddressAndPort toInternal(
      org.apache.cassandra.stargate.locator.InetAddressAndPort external) {
    return InetAddressAndPort.getByAddressOverrideDefaults(external.address, external.port);
  }

  public static org.apache.cassandra.utils.MD5Digest toInternal(MD5Digest id) {
    return org.apache.cassandra.utils.MD5Digest.wrap(id.bytes);
  }

  public static MD5Digest toExternal(org.apache.cassandra.utils.MD5Digest id) {
    return MD5Digest.wrap(id.bytes);
  }

  public static Map<org.apache.cassandra.stargate.locator.InetAddressAndPort, RequestFailureReason>
      toExternal(
          Map<InetAddressAndPort, org.apache.cassandra.exceptions.RequestFailureReason> internal) {
    Map<org.apache.cassandra.stargate.locator.InetAddressAndPort, RequestFailureReason> external =
        new HashMap<>(internal.size());
    for (Map.Entry<InetAddressAndPort, org.apache.cassandra.exceptions.RequestFailureReason> entry :
        internal.entrySet()) {
      InetAddressAndPort addressAndPort = entry.getKey();
      org.apache.cassandra.exceptions.RequestFailureReason internalReason = entry.getValue();
      RequestFailureReason externalReason = RequestFailureReason.fromCode(internalReason.code);
      // 16-Aug-2023, jsc: Only some RequestFailureReason have "code" to allow automatic
      //    mapping, so we need to do manually map the rest. Fortunately there is a unit test
      //    (ConversionTest.allKnownCode()) that verifies that mapping exists for all internal
      //    reasons.
      if (externalReason == RequestFailureReason.UNKNOWN) {
        switch (internalReason) {
          case READ_SIZE:
            externalReason = RequestFailureReason.READ_SIZE;
            break;
          case NODE_DOWN:
            externalReason = RequestFailureReason.NODE_DOWN;
            break;
        }
      }
      external.put(
          org.apache.cassandra.stargate.locator.InetAddressAndPort.getByAddressOverrideDefaults(
              addressAndPort.getAddress(), addressAndPort.addressBytes, addressAndPort.getPort()),
          externalReason);
    }
    return external;
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
        return new UnavailableException(
            ue.getMessage(), toExternal(ue.consistency), ue.required, ue.alive);
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
      case CDC_WRITE_FAILURE:
        return addSuppressed(new CDCWriteException(e.getMessage()), e);
      case CAS_WRITE_UNKNOWN:
        org.apache.cassandra.exceptions.CasWriteUnknownResultException cwe =
            (org.apache.cassandra.exceptions.CasWriteUnknownResultException) e;
        return addSuppressed(
            new CasWriteUnknownResultException(
                toExternal(cwe.consistency), cwe.received, cwe.blockFor),
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
    List<Column> columns = toColumns(metadata.names);

    EnumSet<Result.Flag> flags = EnumSet.noneOf(Result.Flag.class);
    metadata.getFlags().forEach(f -> flags.add(Result.Flag.fromId(f.ordinal() + 1)));

    ByteBuffer pagingState = null;
    if (version != null && metadata.getPagingState() != null) {
      pagingState = metadata.getPagingState().serialize(version);
    }

    MD5Digest resultMetadataId = getResultMetadataId(metadata);
    // 08-Sep-2023: IMPORTANT! must pass column count (2nd arg) explicitly;
    //    see https://github.com/stargate/stargate/pull/2760 for details
    return new Result.ResultMetadata(
        flags, metadata.getColumnCount(), columns, resultMetadataId, pagingState);
  }

  public static Result.PreparedMetadata toPreparedMetadata(
      List<ColumnSpecification> names, short[] indexes) {
    List<Column> columns = toColumns(names);

    EnumSet<Result.Flag> flags = EnumSet.noneOf(Result.Flag.class);
    if (!names.isEmpty() && ColumnSpecification.allInSameTable(names)) {
      flags.add(Result.Flag.GLOBAL_TABLES_SPEC);
    }

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
    return toResult(resultMessage, version, true);
  }

  public static Result toResult(
      ResultMessage resultMessage,
      org.apache.cassandra.transport.ProtocolVersion version,
      boolean includeTracingInfo) {
    Result result = toResultInternal(resultMessage, version);
    if (includeTracingInfo) {
      result.setTracingId(ReflectionUtils.getTracingId(resultMessage).asUUID());
    }
    return result;
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
    } else if (t instanceof org.apache.cassandra.transport.ProtocolException) {
      // Note that ProtocolException is not a CassandraException
      org.apache.cassandra.transport.ProtocolException ex =
          (org.apache.cassandra.transport.ProtocolException) t;
      return new ProtocolException(t.getMessage(), toExternal(ex.getForcedProtocolVersion()));
    } else if (t instanceof org.apache.cassandra.transport.ServerError) {
      // Nor is ServerError
      return new ServerError(t.getMessage());
    }
    return new ServerError(t);
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
                  .allTypes().stream()
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

  private static List<Column> toColumns(List<ColumnSpecification> columnSpecifications) {
    // if null or empty, return empty result
    if (columnSpecifications == null || columnSpecifications.isEmpty()) {
      return Collections.emptyList();
    }

    // otherwise, transform all
    List<Column> result = new ArrayList<>(columnSpecifications.size());
    for (ColumnSpecification c : columnSpecifications) {
      Column column =
          ImmutableColumn.of(
              c.ksName, c.cfName, c.name.toString(), null, getTypeFromInternal(c.type));
      result.add(column);
    }
    return result;
  }

  private static MD5Digest getResultMetadataId(ResultSet.ResultMetadata metadata) {
    if (metadata.getResultMetadataId() == null) {
      return null;
    }
    return Conversion.toExternal(metadata.getResultMetadataId());
  }
}
