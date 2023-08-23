package io.stargate.it.cql.compatibility.protocolv4;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.KeycloakContainer;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.junit.jupiter.Testcontainers;

@StargateSpec(parametersCustomizer = "buildParameters")
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec(
    initQueries = {
      "CREATE ROLE IF NOT EXISTS 'web_user' WITH PASSWORD = 'web_user' AND LOGIN = TRUE",
      "CREATE KEYSPACE IF NOT EXISTS store2 WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}",
      "CREATE TABLE IF NOT EXISTS store2.shopping_cart (userid text, item_count int, last_update_timestamp timestamp, PRIMARY KEY (userid, last_update_timestamp));",
      "INSERT INTO store2.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('9876', 2, toTimeStamp(now()))",
      "INSERT INTO store2.shopping_cart (userid, item_count, last_update_timestamp) VALUES ('1234', 5, toTimeStamp(now()))",
      "GRANT MODIFY ON TABLE store2.shopping_cart TO web_user",
      "GRANT SELECT ON TABLE store2.shopping_cart TO web_user",
    },
    customOptions = "applyProtocolVersion")
@Testcontainers(disabledWithoutDocker = true)
public class JwtAuthTest extends BaseIntegrationTest {

  private final String keyspaceName = "store2";
  private final String tableName = "shopping_cart";

  private static String authToken;
  private static KeycloakContainer keycloakContainer;

  public static void applyProtocolVersion(OptionsMap config) {
    config.put(TypedDriverOption.PROTOCOL_VERSION, "V4");
  }

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) throws IOException {
    keycloakContainer = new KeycloakContainer();
    keycloakContainer.initKeycloakContainer();

    builder.enableAuth(true);
    builder.putSystemProperties("stargate.auth_id", "AuthJwtService");
    builder.putSystemProperties("stargate.cql_use_auth_service", "true");
    builder.putSystemProperties("stargate.cql_token_max_length", "4096");
    builder.putSystemProperties(
        "stargate.auth.jwt_provider_url",
        String.format(
            "%s/auth/realms/stargate/protocol/openid-connect/certs", keycloakContainer.host()));
  }

  @AfterAll
  public static void teardown() {
    keycloakContainer.stop();
  }

  @BeforeEach
  public void setup(CqlSession session) throws IOException {
    // Must recreate every time because some methods alter the schema
    session.execute("DROP TABLE IF EXISTS jwt_auth_test");
    session.execute("CREATE TABLE jwt_auth_test (k text PRIMARY KEY, v text)");

    authToken = keycloakContainer.generateJWT();
  }

  @Test
  public void invalidCredentials(CqlSessionBuilder builder) {
    assertThatThrownBy(() -> builder.withAuthCredentials("invalid", "invalid").build())
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining("Provided username invalid and/or password are incorrect");
  }

  @Test
  public void tokenAuthentication(CqlSessionBuilder builder) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      Row row = tokenSession.execute("SELECT * FROM system.local").one();
      assertThat(row).isNotNull();
    }
  }

  @Test
  public void useKeyspace(CqlSessionBuilder builder) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      assertThatThrownBy(() -> tokenSession.execute("SELECT * FROM " + tableName))
          .isInstanceOf(InvalidQueryException.class)
          .hasMessage(
              "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");

      // Switch to keyspace and retry query
      tokenSession.execute(String.format("USE %s", keyspaceName));
      Row row = tokenSession.execute("SELECT * FROM " + tableName).one();
      assertThat(row).isNotNull();
    }
  }

  @Test
  public void createKeyspaceUnauthorized(CqlSessionBuilder builder) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      assertThatThrownBy(
              () ->
                  tokenSession.execute(
                      "CREATE KEYSPACE IF NOT EXISTS foo WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'}"))
          .isInstanceOf(UnauthorizedException.class)
          .hasMessage(
              "User web_user has no CREATE permission on <all keyspaces> or any of its parents");
    }
  }

  @Test
  public void createTableUnauthorized(
      CqlSessionBuilder builder, @TestKeyspace CqlIdentifier keyspaceId) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {

      String errorMessage =
          "User web_user has no CREATE permission on <keyspace ks_\\d*_JwtAuthTest> or any of its parents";

      if (backend.isDse() || isCassandra41()) {
        errorMessage =
            "User web_user has no CREATE permission on <all tables in ks_\\d*_JwtAuthTest> or any of its parents";
      }

      assertThatThrownBy(
              () ->
                  tokenSession.execute(
                      String.format(
                          "CREATE TABLE IF NOT EXISTS %s.test (k INT PRIMARY KEY)",
                          keyspaceId.asCql(false))))
          .isInstanceOf(UnauthorizedException.class)
          .hasMessageMatching(errorMessage);
    }
  }

  @Test
  public void insertPreparedStatement(CqlSessionBuilder builder) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      PreparedStatement prepared =
          tokenSession.prepare(
              String.format(
                  "INSERT INTO %s.%s (userid, item_count, last_update_timestamp) VALUES (?, ?, ?)",
                  keyspaceName, tableName));

      Instant now = Instant.now();
      tokenSession.execute(prepared.bind("9876", 0, now));

      Row row =
          tokenSession
              .execute(
                  String.format(
                      "SELECT * FROM %s.%s WHERE userid=? AND last_update_timestamp=?",
                      keyspaceName, tableName),
                  "9876",
                  now)
              .one();
      assertThat(row).isNotNull();
      assertThat(row.getInt("item_count")).isEqualTo(0);
    }
  }

  @Test
  public void insertPreparedStatementNotAuthorized(
      CqlSessionBuilder builder, @TestKeyspace CqlIdentifier keyspaceId) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      PreparedStatement prepared =
          tokenSession.prepare(
              String.format(
                  "INSERT INTO %s.jwt_auth_test (k, v) VALUES (?, ?)", keyspaceId.asCql(false)));

      String errorMessage =
          "User web_user has no MODIFY permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents";

      if (backend.isDse()) {
        errorMessage =
            "User web_user has no UPDATE permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents";
      }

      assertThatThrownBy(() -> tokenSession.execute(prepared.bind("foo", "bar")))
          .isInstanceOf(UnauthorizedException.class)
          .hasMessageMatching(errorMessage);
    }
  }

  @Test
  public void insert(CqlSessionBuilder builder) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      Instant now = Instant.now();
      tokenSession.execute(
          String.format(
              "INSERT INTO %s.%s (userid, item_count, last_update_timestamp) VALUES (?, ?, ?)",
              keyspaceName, tableName),
          "9876",
          1,
          now);

      Row row =
          tokenSession
              .execute(
                  String.format(
                      "SELECT * FROM %s.%s WHERE userid=? AND last_update_timestamp=?",
                      keyspaceName, tableName),
                  "9876",
                  now)
              .one();
      assertThat(row).isNotNull();
      assertThat(row.getInt("item_count")).isEqualTo(1);
    }
  }

  @Test
  public void insertNotAuthorized(
      CqlSessionBuilder builder, @TestKeyspace CqlIdentifier keyspaceId) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {

      String errorMessage =
          "User web_user has no MODIFY permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents";

      if (backend.isDse()) {
        errorMessage =
            "User web_user has no UPDATE permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents";
      }

      assertThatThrownBy(
              () ->
                  tokenSession.execute(
                      String.format(
                          "INSERT INTO %s.jwt_auth_test (k, v) VALUES (?, ?)",
                          keyspaceId.asCql(false)),
                      "foo",
                      "bar"))
          .isInstanceOf(UnauthorizedException.class)
          .hasMessageMatching(errorMessage);
    }
  }

  @Test
  public void selectNotAuthorized(
      CqlSessionBuilder builder, @TestKeyspace CqlIdentifier keyspaceId) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      assertThatThrownBy(
              () ->
                  tokenSession
                      .execute(
                          String.format("SELECT * FROM %s.jwt_auth_test", keyspaceId.asCql(false)))
                      .one())
          .isInstanceOf(UnauthorizedException.class)
          .hasMessageMatching(
              "User web_user has no SELECT permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents");
    }
  }

  @Test
  public void update(CqlSessionBuilder builder) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {
      Instant now = Instant.now();
      tokenSession.execute(
          String.format(
              "INSERT INTO %s.%s (userid, item_count, last_update_timestamp) VALUES (?, ?, ?)",
              keyspaceName, tableName),
          "9876",
          2,
          now);

      tokenSession.execute(
          String.format(
              "UPDATE %s.%s set item_count = ? WHERE userid = ? AND last_update_timestamp = ?",
              keyspaceName, tableName),
          3,
          "9876",
          now);

      Row row =
          tokenSession
              .execute(
                  String.format(
                      "SELECT * FROM %s.%s WHERE userid=? AND last_update_timestamp=?",
                      keyspaceName, tableName),
                  "9876",
                  now)
              .one();
      assertThat(row).isNotNull();
      assertThat(row.getInt("item_count")).isEqualTo(3);
    }
  }

  @Test
  public void updateNotAuthorized(
      CqlSessionBuilder builder, @TestKeyspace CqlIdentifier keyspaceId) {
    try (CqlSession tokenSession = builder.withAuthCredentials("token", authToken).build()) {

      String errorMessage =
          "User web_user has no MODIFY permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents";

      if (backend.isDse()) {
        errorMessage =
            "User web_user has no UPDATE permission on <table ks_\\d*_JwtAuthTest.jwt_auth_test> or any of its parents";
      }

      assertThatThrownBy(
              () ->
                  tokenSession.execute(
                      String.format(
                          "UPDATE %s.jwt_auth_test set v = ? where k = ?", keyspaceId.asCql(false)),
                      "bar",
                      "foo"))
          .isInstanceOf(UnauthorizedException.class)
          .hasMessageMatching(errorMessage);
    }
  }

  @Test
  public void createAlterDropTable(CqlSession session, @TestKeyspace CqlIdentifier keyspaceId) {
    session.execute("CREATE TABLE foo(k int PRIMARY KEY)");

    assertThat(session.getMetadata().getKeyspace(keyspaceId))
        .hasValueSatisfying(ks -> assertThat(ks.getTable("foo")).isNotEmpty());

    session.execute("ALTER TABLE foo ADD v int");
    Optional<TableMetadata> updatedTable =
        session.getMetadata().getKeyspace(keyspaceId).flatMap(keyspace -> keyspace.getTable("foo"));
    assertThat(updatedTable).isPresent();

    assertThat(updatedTable.get().getName().asInternal()).isEqualTo("foo");
    assertThat(updatedTable.get().getColumns().keySet())
        .containsExactly(CqlIdentifier.fromInternal("k"), CqlIdentifier.fromInternal("v"));

    assertThat(session.getMetadata().getKeyspace(keyspaceId).flatMap(ks -> ks.getTable("foo")))
        .hasValue(updatedTable.get());

    session.execute("DROP TABLE foo");
    assertThat(session.getMetadata().getKeyspace(keyspaceId))
        .hasValueSatisfying(ks -> assertThat(ks.getTable("foo")).isEmpty());
  }
}
