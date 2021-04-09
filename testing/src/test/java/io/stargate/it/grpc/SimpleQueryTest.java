package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.SkipIfBundleNotAvailable;
import io.stargate.it.storage.StargateEnvironmentInfo;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.Result;
import io.stargate.proto.StargateGrpc;
import org.junit.jupiter.api.Test;

@SkipIfBundleNotAvailable(bundleName = "grpc")
public class SimpleQueryTest extends BaseOsgiIntegrationTest {
  @Test
  public void simpleQueryTest(StargateEnvironmentInfo stargate) {
    // TODO: Consider reusing channel in multiple tests
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(stargate.nodes().get(0).seedAddress(), 8090)
            .usePlaintext()
            .build();
    StargateGrpc.StargateBlockingStub stub = StargateGrpc.newBlockingStub(channel);

    // TODO: Get and check a "real" result
    Result result = stub.execute(Query.newBuilder().setCql("SELECT * FROM system.local").build());
    assertThat(result.hasError()).isFalse();
    assertThat(result.hasEmpty()).isTrue();
  }
}
