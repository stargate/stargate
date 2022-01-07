package io.stargate.grpc.service;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import io.stargate.proto.QueryOuterClass;
import java.util.concurrent.CompletionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.exceptions.AlreadyExistsException;
import org.apache.cassandra.stargate.exceptions.CasWriteUnknownResultException;
import org.apache.cassandra.stargate.exceptions.FunctionExecutionException;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.exceptions.ReadFailureException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.UnavailableException;
import org.apache.cassandra.stargate.exceptions.UnhandledClientException;
import org.apache.cassandra.stargate.exceptions.WriteFailureException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ExceptionHandler.class);

  static final Metadata.Key<QueryOuterClass.Unavailable> UNAVAILABLE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.Unavailable.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.WriteTimeout> WRITE_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.WriteTimeout.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.ReadTimeout> READ_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.ReadTimeout.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.ReadFailure> READ_FAILURE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.ReadFailure.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.FunctionFailure> FUNCTION_FAILURE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.FunctionFailure.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.WriteFailure> WRITE_FAILURE_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.WriteFailure.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.AlreadyExists> ALREADY_EXISTS_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.AlreadyExists.getDefaultInstance());
  static final Metadata.Key<QueryOuterClass.CasWriteUnknown> CAS_WRITE_UNKNOWN_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.CasWriteUnknown.getDefaultInstance());

  protected abstract void onError(
      @Nullable Status status, @Nonnull Throwable throwable, @Nullable Metadata trailer);

  /**
   * It handles the throwable that can be gRPC {@link StatusRuntimeException}, persistence related
   * {@link PersistenceException} or unknown. It recursively unwraps the underlying exception if it
   * is {@link CompletionException} or {@link MessageHandler.ExceptionWithIdempotencyInfo}. Finally,
   * it converts the exception to a gRPC specific response using the {@link
   * StreamObserver#onError(Throwable)} method.
   *
   * @param throwable
   */
  public void handleException(Throwable throwable) {
    if (throwable instanceof CompletionException
        || throwable instanceof MessageHandler.ExceptionWithIdempotencyInfo) {
      handleException(throwable.getCause());
    } else if (throwable instanceof StatusException
        || throwable instanceof StatusRuntimeException) {
      onError(throwable);
    } else if (throwable instanceof UnhandledClientException) {
      onError(Status.UNAVAILABLE, throwable);
    } else if (throwable instanceof PersistenceException) {
      handlePersistenceException((PersistenceException) throwable);
    } else {
      LOG.error("Unhandled error returning UNKNOWN to the client", throwable);
      onError(Status.UNKNOWN.withDescription(throwable.getMessage()).withCause(throwable));
    }
  }

  private void handlePersistenceException(PersistenceException pe) {
    switch (pe.code()) {
      case SERVER_ERROR:
      case PROTOCOL_ERROR: // Fallthrough
      case UNPREPARED: // Fallthrough
        onError(Status.INTERNAL, pe);
        break;
      case INVALID:
      case SYNTAX_ERROR: // Fallthrough
        onError(Status.INVALID_ARGUMENT, pe);
        break;
      case TRUNCATE_ERROR:
      case CDC_WRITE_FAILURE: // Fallthrough
        onError(Status.ABORTED, pe);
        break;
      case BAD_CREDENTIALS:
        onError(Status.UNAUTHENTICATED, pe);
        break;
      case UNAVAILABLE:
        handleUnavailable((UnavailableException) pe);
        break;
      case OVERLOADED:
        onError(Status.RESOURCE_EXHAUSTED, pe);
        break;
      case IS_BOOTSTRAPPING:
        onError(Status.UNAVAILABLE, pe);
        break;
      case WRITE_TIMEOUT:
        handleWriteTimeout((WriteTimeoutException) pe);
        break;
      case READ_TIMEOUT:
        handleReadTimeout((ReadTimeoutException) pe);
        break;
      case READ_FAILURE:
        handleReadFailure((ReadFailureException) pe);
        break;
      case FUNCTION_FAILURE:
        handleFunctionExecutionException((FunctionExecutionException) pe);
        break;
      case WRITE_FAILURE:
        handleWriteFailure((WriteFailureException) pe);
        break;
      case CAS_WRITE_UNKNOWN:
        handleCasWriteUnknown((CasWriteUnknownResultException) pe);
        break;
      case UNAUTHORIZED:
        onError(Status.PERMISSION_DENIED, pe);
        break;
      case CONFIG_ERROR:
        onError(Status.FAILED_PRECONDITION, pe);
        break;
      case ALREADY_EXISTS:
        handleAlreadyExists((AlreadyExistsException) pe);
        break;
      default:
        LOG.error("Unhandled persistence exception returning UNKNOWN to the client", pe);
        onError(Status.UNKNOWN, pe);
        break;
    }
  }

  private void handleUnavailable(UnavailableException ue) {
    onError(
        Status.UNAVAILABLE,
        ue,
        makeTrailer(
            UNAVAILABLE_KEY,
            QueryOuterClass.Unavailable.newBuilder()
                .setConsistencyValue(ue.consistency.code)
                .setAlive(ue.alive)
                .setRequired(ue.required)
                .build()));
  }

  private void handleWriteTimeout(WriteTimeoutException wte) {
    onError(
        Status.DEADLINE_EXCEEDED,
        wte,
        makeTrailer(
            WRITE_TIMEOUT_KEY,
            QueryOuterClass.WriteTimeout.newBuilder()
                .setConsistencyValue(wte.consistency.code)
                .setBlockFor(wte.blockFor)
                .setReceived(wte.received)
                .setWriteType(wte.writeType.name())
                .build()));
  }

  private void handleReadTimeout(ReadTimeoutException rte) {
    onError(
        Status.DEADLINE_EXCEEDED,
        rte,
        makeTrailer(
            READ_TIMEOUT_KEY,
            QueryOuterClass.ReadTimeout.newBuilder()
                .setConsistencyValue(rte.consistency.code)
                .setBlockFor(rte.blockFor)
                .setReceived(rte.received)
                .setDataPresent(rte.dataPresent)
                .build()));
  }

  private void handleReadFailure(ReadFailureException rfe) {
    onError(
        Status.ABORTED,
        rfe,
        makeTrailer(
            READ_FAILURE_KEY,
            QueryOuterClass.ReadFailure.newBuilder()
                .setConsistencyValue(rfe.consistency.code)
                .setNumFailures(rfe.failureReasonByEndpoint.size())
                .setBlockFor(rfe.blockFor)
                .setReceived(rfe.received)
                .setDataPresent(rfe.dataPresent)
                .build()));
  }

  private void handleFunctionExecutionException(FunctionExecutionException fee) {
    onError(
        Status.FAILED_PRECONDITION,
        fee,
        makeTrailer(
            FUNCTION_FAILURE_KEY,
            QueryOuterClass.FunctionFailure.newBuilder()
                .setKeyspace(fee.functionName.keyspace)
                .setFunction(fee.functionName.name)
                .addAllArgTypes(fee.argTypes)
                .build()));
  }

  private void handleWriteFailure(WriteFailureException wfe) {
    onError(
        Status.ABORTED,
        wfe,
        makeTrailer(
            WRITE_FAILURE_KEY,
            QueryOuterClass.WriteFailure.newBuilder()
                .setConsistencyValue(wfe.consistency.code)
                .setNumFailures(wfe.failureReasonByEndpoint.size())
                .setBlockFor(wfe.blockFor)
                .setReceived(wfe.received)
                .setWriteType(wfe.writeType.name())
                .build()));
  }

  private void handleCasWriteUnknown(CasWriteUnknownResultException cwe) {
    onError(
        Status.ABORTED,
        cwe,
        makeTrailer(
            CAS_WRITE_UNKNOWN_KEY,
            QueryOuterClass.CasWriteUnknown.newBuilder()
                .setConsistencyValue(cwe.consistency.code)
                .setBlockFor(cwe.blockFor)
                .setReceived(cwe.received)
                .build()));
  }

  private void handleAlreadyExists(AlreadyExistsException aee) {
    onError(
        Status.ALREADY_EXISTS,
        aee,
        makeTrailer(
            ALREADY_EXISTS_KEY,
            QueryOuterClass.AlreadyExists.newBuilder()
                .setKeyspace(aee.ksName)
                .setTable(aee.cfName)
                .build()));
  }

  private void onError(Status status, Throwable throwable) {
    onError(status, throwable, null);
  }

  private void onError(Status status) {
    onError(status, status.asRuntimeException());
  }

  private void onError(Throwable throwable) {
    onError(null, throwable, null);
  }

  private <T> Metadata makeTrailer(Metadata.Key<T> key, T value) {
    Metadata trailer = new Metadata();
    trailer.put(key, value);
    return trailer;
  }
}
