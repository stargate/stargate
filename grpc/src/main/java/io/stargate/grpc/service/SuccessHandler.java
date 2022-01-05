package io.stargate.grpc.service;

import io.stargate.proto.QueryOuterClass;

public interface SuccessHandler {
  void handleResponse(QueryOuterClass.Response response);
}
