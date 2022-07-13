package io.stargate.sgv2.api.common.grpc;

import static io.stargate.bridge.proto.QueryOuterClass.Query;
import static io.stargate.bridge.proto.QueryOuterClass.Response;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import java.util.Optional;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class StargateBridgeClientTest {

  @InjectMock SchemaManager schemaManager;

  StargateBridge reactiveBridge;

  @InjectMock StargateRequestInfo requestInfo;

  @Inject StargateBridgeClient client;

  @BeforeEach
  public void setup() {
    reactiveBridge = mock(StargateBridge.class);
    when(requestInfo.getStargateBridge()).thenReturn(reactiveBridge);
  }

  @Test
  public void shouldForwardCqlQuery() {
    // Given
    Query query = Query.getDefaultInstance();
    Response expectedResponse = Response.getDefaultInstance();
    when(reactiveBridge.executeQuery(query)).thenReturn(Uni.createFrom().item(expectedResponse));

    // When
    Response actualResponse = client.executeQuery(query);

    // Then
    assertThat(actualResponse).isSameAs(expectedResponse);
  }

  @Test
  public void shouldForwardSchemaLookup() {
    // Given
    String keyspaceName = "ks";
    CqlKeyspaceDescribe expectedKeyspace = CqlKeyspaceDescribe.getDefaultInstance();
    when(schemaManager.getKeyspace(keyspaceName))
        .thenReturn(Uni.createFrom().item(expectedKeyspace));

    // When
    Optional<CqlKeyspaceDescribe> actualKeyspace = client.getKeyspace(keyspaceName, false);

    // Then
    assertThat(actualKeyspace).hasValueSatisfying(ks -> assertThat(ks).isSameAs(expectedKeyspace));
  }
}
