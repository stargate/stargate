/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc.service;

import static io.stargate.grpc.retries.RetryDecision.RETHROW;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.StringValue;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.stargate.db.Result;
import io.stargate.db.Result.SchemaChange;
import io.stargate.db.Result.SchemaChangeMetadata;
import io.stargate.grpc.retries.DefaultRetryPolicy;
import io.stargate.grpc.retries.RetryDecision;
import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.QueryOuterClass.Response;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.apache.cassandra.stargate.db.ConsistencyLevel;
import org.apache.cassandra.stargate.exceptions.PersistenceException;
import org.apache.cassandra.stargate.exceptions.ReadTimeoutException;
import org.apache.cassandra.stargate.exceptions.WriteTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MessageHandler<MessageT extends GeneratedMessageV3> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryPolicy.class);

  protected static final ConsistencyLevel DEFAULT_TRACING_CONSISTENCY = ConsistencyLevel.ONE;
  protected final MessageT message;
  private final DefaultRetryPolicy retryPolicy;
  private final ExceptionHandler exceptionHandler;

  protected MessageHandler(MessageT message, ExceptionHandler exceptionHandler) {
    this.message = message;
    this.retryPolicy = new DefaultRetryPolicy();
    this.exceptionHandler = exceptionHandler;
  }

  /** Performs any necessary validation on the message before execution starts. */
  protected abstract void validate() throws Exception;

  /** Executes the CQL querie(s) for this operation. */
  protected abstract CompletionStage<Response> executeQuery();

  public void handle() {
    try {
      validate();
      executeWithRetry(0);

    } catch (Throwable t) {
      exceptionHandler.handleException(t);
    }
  }

  private void executeWithRetry(int retryCount) {
    executeQuery()
        .whenComplete(
            (response, error) -> {
              if (error != null) {
                RetryDecision decision = shouldRetry(error, retryCount);
                switch (decision) {
                  case RETRY:
                    executeWithRetry(retryCount + 1);
                    break;
                  case RETHROW:
                    exceptionHandler.handleException(error);
                    break;
                  default:
                    throw new UnsupportedOperationException(
                        "The retry decision: " + decision + " is not supported.");
                }
              } else {
                setSuccess(response);
              }
            });
  }

  private RetryDecision shouldRetry(Throwable throwable, int retryCount) {
    Optional<PersistenceException> cause = unwrapCause(throwable);
    if (!cause.isPresent()) {
      return RETHROW;
    }
    PersistenceException pe = cause.get();
    switch (pe.code()) {
      case READ_TIMEOUT:
        return retryPolicy.onReadTimeout((ReadTimeoutException) pe, retryCount);
      case WRITE_TIMEOUT:
        if (isIdempotent(throwable)) {
          return retryPolicy.onWriteTimeout((WriteTimeoutException) pe, retryCount);
        } else {
          return RETHROW;
        }
      default:
        return RETHROW;
    }
  }

  private boolean isIdempotent(Throwable throwable) {
    Optional<ExceptionWithIdempotencyInfo> exception =
        unwrapExceptionWithIdempotencyInfo(throwable);
    return exception.map(ExceptionWithIdempotencyInfo::isIdempotent).orElse(false);
  }

  private Optional<ExceptionWithIdempotencyInfo> unwrapExceptionWithIdempotencyInfo(
      Throwable throwable) {
    if (throwable instanceof CompletionException) {
      return unwrapExceptionWithIdempotencyInfo(throwable.getCause());
    } else if (throwable instanceof ExceptionWithIdempotencyInfo) {
      return Optional.of((ExceptionWithIdempotencyInfo) throwable);
    } else {
      return Optional.empty();
    }
  }

  protected Optional<PersistenceException> unwrapCause(Throwable throwable) {
    if (throwable instanceof CompletionException
        || throwable instanceof ExceptionWithIdempotencyInfo) {
      return unwrapCause(throwable.getCause());
    } else if (throwable instanceof StatusException
        || throwable instanceof StatusRuntimeException) {
      return Optional.empty();
    } else if (throwable instanceof PersistenceException) {
      return Optional.of((PersistenceException) throwable);
    } else {
      return Optional.empty();
    }
  }

  protected abstract void setSuccess(Response response);

  protected Response.Builder makeResponseBuilder(Result result) {
    Response.Builder resultBuilder = Response.newBuilder();
    List<String> warnings = result.getWarnings();
    if (warnings != null) {
      resultBuilder.addAllWarnings(warnings);
    }
    return resultBuilder;
  }

  protected <V> CompletionStage<V> failedFuture(Exception e, boolean isIdempotent) {
    CompletableFuture<V> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new ExceptionWithIdempotencyInfo(e, isIdempotent));
    return failedFuture;
  }

  protected static QueryOuterClass.SchemaChange buildSchemaChange(SchemaChange result) {
    SchemaChangeMetadata metadata = result.metadata;
    QueryOuterClass.SchemaChange.Builder schemaChangeBuilder =
        QueryOuterClass.SchemaChange.newBuilder()
            .setChangeType(QueryOuterClass.SchemaChange.Type.valueOf(metadata.change))
            .setTarget(QueryOuterClass.SchemaChange.Target.valueOf(metadata.target))
            .setKeyspace(metadata.keyspace);
    if (metadata.name != null) {
      schemaChangeBuilder.setName(StringValue.of(metadata.name));
    }
    if (metadata.argTypes != null) {
      schemaChangeBuilder.addAllArgumentTypes(metadata.argTypes);
    }
    return schemaChangeBuilder.build();
  }

  public static class ExceptionWithIdempotencyInfo extends Exception {

    private final boolean isIdempotent;

    public ExceptionWithIdempotencyInfo(Exception e, boolean isIdempotent) {
      super(e);
      this.isIdempotent = isIdempotent;
    }

    public boolean isIdempotent() {
      return isIdempotent;
    }
  }
}
