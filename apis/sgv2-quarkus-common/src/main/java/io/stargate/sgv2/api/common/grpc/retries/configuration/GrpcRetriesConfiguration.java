package io.stargate.sgv2.api.common.grpc.retries.configuration;

import io.grpc.Status;
import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.api.common.config.GrpcConfig;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;
import io.stargate.sgv2.api.common.grpc.retries.impl.StatusCodesRetryPredicate;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

public class GrpcRetriesConfiguration {

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.grpc.retries.policy", stringValue = "status-codes")
  GrpcRetryPredicate statusCodes(GrpcConfig config) {
    List<Status.Code> statusCodes = config.retries().statusCodes();
    return new StatusCodesRetryPredicate(statusCodes);
  }
}
