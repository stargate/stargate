package io.stargate.sgv2.api.common.grpc.retries;

import io.grpc.StatusRuntimeException;
import java.util.function.Predicate;

/** Simple predicate that can define what {@link StatusRuntimeException}s should be retried. */
public interface GrpcRetryPredicate extends Predicate<StatusRuntimeException> {}
