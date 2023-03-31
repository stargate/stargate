package io.stargate.sgv2.api.common.grpc.retries.impl;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;
import java.util.List;

/** {@link GrpcRetryPredicate} based on status code matching. */
public class StatusCodesRetryPredicate implements GrpcRetryPredicate {

  private final List<Status.Code> statusCodes;

  public StatusCodesRetryPredicate(List<Status.Code> statusCodes) {
    this.statusCodes = statusCodes;
  }

  /** {@inheritDoc} */
  @Override
  public boolean test(StatusRuntimeException e) {
    if (null != statusCodes) {
      return statusCodes.contains(e.getStatus().getCode());
    }
    return false;
  }
}
