package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.stargate.grpc.StargateBearerToken;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.proto.QueryOuterClass.SchemaChange;
import io.stargate.proto.QueryOuterClass.SchemaChange.Target;
import io.stargate.proto.QueryOuterClass.SchemaChange.Type;
import io.stargate.proto.Schema.GetSchemaNotificationsParams;
import io.stargate.proto.Schema.SchemaNotification;
import io.stargate.proto.StargateGrpc;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CqlSessionExtension.class)
public class SchemaNotificationsTest extends BaseIntegrationTest {

  private StargateGrpc.StargateStub asyncStub;

  @BeforeEach
  public void setup(StargateConnectionInfo cluster) throws IOException {
    String seedAddress = cluster.seedAddress();
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(seedAddress, 8090).usePlaintext().build();
    asyncStub =
        StargateGrpc.newStub(channel)
            .withCallCredentials(new StargateBearerToken("mockAdminToken"));
  }

  @Test
  @DisplayName("Should receive table changes")
  public void tableChangesTest(CqlSession session, @TestKeyspace CqlIdentifier keyspace)
      throws Exception {
    // Given
    SchemaNotificationObserver observer = new SchemaNotificationObserver();
    asyncStub.getSchemaNotifications(GetSchemaNotificationsParams.newBuilder().build(), observer);
    // getSchemaChanges is async, wait a bit to ensure that the event listener has been registered
    TimeUnit.MILLISECONDS.sleep(500);

    // When
    session.execute("CREATE TABLE foo(k int PRIMARY KEY)");
    // Then
    assertNextChange(observer, Type.CREATED, Target.TABLE, keyspace, "foo");

    // When
    session.execute("ALTER TABLE foo ADD v int");
    // Then
    assertNextChange(observer, Type.UPDATED, Target.TABLE, keyspace, "foo");

    // When
    session.execute("DROP TABLE foo");
    // Then
    assertNextChange(observer, Type.DROPPED, Target.TABLE, keyspace, "foo");
  }

  private void assertNextChange(
      SchemaNotificationObserver observer,
      Type type,
      Target target,
      CqlIdentifier keyspaceId,
      String name) {
    await().until(() -> observer.hasNext() || observer.error != null);
    if (observer.error != null) {
      throw observer.error;
    }
    SchemaNotification notification = observer.next();

    SchemaChange change = notification.getChange();
    assertThat(change.getChangeType()).isEqualTo(type);
    assertThat(change.getTarget()).isEqualTo(target);
    assertThat(change.getKeyspace()).isEqualTo(keyspaceId.asInternal());
    assertThat(change.getName().getValue()).isEqualTo(name);
    assertThat(change.getArgumentTypesList()).isEmpty();

    boolean isKeyspaceDrop =
        change.getChangeType() == Type.DROPPED && change.getTarget() == Target.KEYSPACE;
    assertThat(notification.hasKeyspace()).isEqualTo(!isKeyspaceDrop);
  }

  static class SchemaNotificationObserver implements StreamObserver<SchemaNotification> {

    private final ConcurrentLinkedQueue<SchemaNotification> changes = new ConcurrentLinkedQueue<>();
    private volatile AssertionError error;

    boolean hasNext() {
      return !changes.isEmpty();
    }

    SchemaNotification next() {
      return changes.poll();
    }

    @Override
    public void onNext(SchemaNotification change) {
      changes.offer(change);
    }

    @Override
    public void onError(Throwable t) {
      error = new AssertionError("Unexpected onError", t);
    }

    @Override
    public void onCompleted() {
      error = new AssertionError("Unexpected onCompleted");
    }
  }
}
