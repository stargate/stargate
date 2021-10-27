package io.stargate.sgv2.restsvc.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.proto.StargateGrpc;
import io.stargate.sgv2.restsvc.impl.RestServiceServerConfiguration.EndpointConfig;
import java.util.concurrent.TimeUnit;

public class GrpcClientFactory {
  private final EndpointConfig grpcEndpoint;
  private final ManagedChannel grpcChannel;

  private GrpcClientFactory(EndpointConfig grpcEndpoint, ManagedChannel grpcChannel) {
    this.grpcEndpoint = grpcEndpoint;
    this.grpcChannel = grpcChannel;
  }

  public static GrpcClientFactory construct(EndpointConfig grpcEndpoint) {
    RestServiceServerConfiguration x;
    ManagedChannelBuilder b =
        ManagedChannelBuilder.forAddress(grpcEndpoint.host, grpcEndpoint.port).directExecutor();
    if (grpcEndpoint.useTLS) {
      b = b.useTransportSecurity();
    } else {
      b = b.usePlaintext();
    }
    return new GrpcClientFactory(grpcEndpoint, b.build());
  }

  public StargateGrpc.StargateBlockingStub constructBlockingStub() {
    return StargateGrpc.newBlockingStub(grpcChannel).withDeadlineAfter(5, TimeUnit.SECONDS);
  }

  public String endpointDesc() {
    return String.valueOf(grpcEndpoint);
  }
}
