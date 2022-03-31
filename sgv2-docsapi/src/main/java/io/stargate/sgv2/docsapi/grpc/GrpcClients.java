package io.stargate.sgv2.docsapi.grpc;

import io.quarkus.grpc.GrpcClient;
import io.stargate.proto.MutinyStargateBridgeGrpc;

import javax.enterprise.context.ApplicationScoped;

/**
 * Bean that holds all the gRPC clients needed, with possibility to enrich the metadata based on the request context.
 */
@ApplicationScoped
public class GrpcClients {

    /**
     * Bridge client. Uses the <code>quarkus.grpc.clients.bridge</code> properties.
     */
    @GrpcClient("bridge")
    MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridge;

    public MutinyStargateBridgeGrpc.MutinyStargateBridgeStub bridgeClient() {
        // TODO add metadata in order to resolve the client for the call
        //  https://quarkus.io/guides/grpc-service-consumption#grpc-headers
        return bridge;
    }
}
