package io.stargate.grpc.service;

import io.stargate.proto.QueryOuterClass;

public interface StreamingSuccessHandler {
  void handleResponse(QueryOuterClass.StreamingResponse response);
}
