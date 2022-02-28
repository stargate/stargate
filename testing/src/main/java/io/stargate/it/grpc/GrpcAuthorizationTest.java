package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.StatusRuntimeException;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.proto.QueryOuterClass;
import java.io.IOException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'read_only_user' WITH PASSWORD = 'read_only_user' AND LOGIN = TRUE",
      "CREATE KEYSPACE IF NOT EXISTS grpc_table_token_test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS grpc_table_token_test.tbl_test (key text PRIMARY KEY, value text);",
      "INSERT INTO grpc_table_token_test.tbl_test (key, value) VALUES ('a', 'alpha')",
      "GRANT SELECT ON KEYSPACE grpc_table_token_test TO read_only_user",
    })
public class GrpcAuthorizationTest extends GrpcIntegrationTest {
  private final String keyspaceName = "grpc_table_token_test";
  private final String tableName = "tbl_test";
  private final String readOnlyUsername = "read_only_user";
  private final String readOnlyPassword = "read_only_user";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private String authUrlBase;

  @BeforeEach
  public void perTestSetup(StargateConnectionInfo cluster) {
    authUrlBase =
        "http://" + cluster.seedAddress() + ":" + 8081; // TODO: make auth port configurable?
  }

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.enableAuth(true);
    builder.putSystemProperties("stargate.auth_id", "AuthTableBasedService");
  }

  @Test
  public void createKeyspaceCheckAuthorization() throws IOException {
    final String keyspace = "ks_grpcAuthnzTest_CreateKS";
    final String createKeyspaceCQL =
        String.format(
            "CREATE KEYSPACE %s WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
            keyspace);

    // First: fail if not authenticated (null token)
    assertThatThrownBy(
            () -> {
              stubWithCallCredentials("not-a-token-that-exists")
                  .executeQuery(
                      QueryOuterClass.Query.newBuilder().setCql(createKeyspaceCQL).build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("UNAUTHENTICATED")
        .hasMessageContaining("Invalid token");

    // Second: also fail if authenticated but not authorized
    final String readOnlyToken = generateReadOnlyToken();
    assertThatThrownBy(
            () -> {
              stubWithCallCredentials(readOnlyToken)
                  .executeQuery(
                      QueryOuterClass.Query.newBuilder().setCql(createKeyspaceCQL).build());
            })
        .isInstanceOf(StatusRuntimeException.class)
        .hasMessageContaining("PERMISSION_DENIED")
        .hasMessageContaining("has no CREATE permission");

    // But succeed for Admin user
    final String adminToken = generateAdminToken();
    QueryOuterClass.Response response =
        stubWithCallCredentials(adminToken)
            .executeQuery(QueryOuterClass.Query.newBuilder().setCql(createKeyspaceCQL).build());
    assertThat(response).isNotNull();
    // not 100% sure if anything is expected; seems like an empty ResultSet
    assertThat(response.getResultSet()).isNotNull();
  }

  private String generateReadOnlyToken() throws IOException {
    return generateAuthToken(authUrlBase, readOnlyUsername, readOnlyPassword);
  }

  private String generateAdminToken() throws IOException {
    return generateAuthToken(authUrlBase, "cassandra", "cassandra");
  }
}
