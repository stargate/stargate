package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.StargateGrpc;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SchemaChangesTest extends BaseIntegrationTest {

  private StargateGrpc.StargateStub asyncStub;

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) throws IOException {
    String seedAddress = cluster.seedAddress();
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(seedAddress, 8090).usePlaintext().build();
    asyncStub =
        StargateGrpc.newStub(channel)
            .withCallCredentials(new StargateBearerToken(RestUtils.getAuthToken(seedAddress)));
  }

  @Test
  @DisplayName("Should receive table changes")
  public void tableChangesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspace) {
    SchemaChangeAccumulator changes = new SchemaChangeAccumulator();
    asyncStub.getSchemaChanges(Empty.newBuilder().build(), changes);

    // TODO why does this not generate an event?
    session.execute("CREATE TABLE foo(k int PRIMARY KEY)");

    session.execute("DROP TABLE foo");
    await().until(changes::hasNext);
    SchemaChange change = changes.next();
    assertThat(change.getChangeType()).isEqualTo(SchemaChange.Type.DROPPED);
    assertThat(change.getTarget()).isEqualTo(SchemaChange.Target.TABLE);
    assertThat(change.getKeyspace()).isEqualTo(keyspace.asInternal());
    assertThat(change.getName().getValue()).isEqualTo("foo");
    assertThat(change.getArgumentTypesList()).isEmpty();
  }

  static class SchemaChangeAccumulator implements StreamObserver<SchemaChange> {

    private final ConcurrentLinkedQueue<SchemaChange> changes = new ConcurrentLinkedQueue<>();

    boolean hasNext() {
      return !changes.isEmpty();
    }

    SchemaChange next() {
      return changes.poll();
    }

    @Override
    public void onNext(SchemaChange change) {
      changes.offer(change);
    }

    @Override
    public void onError(Throwable t) {
      // TODO temporary, remove
      t.printStackTrace();
    }

    @Override
    public void onCompleted() {
      // TODO temporary, remove
      System.out.println("Completed");
    }
  }
}
