package io.stargate.it.http.docsapi;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.ClusterConnectionInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@NotThreadSafe
public class CollectionTest extends BaseOsgiIntegrationTest {
  private String keyspace;
  private CqlSession session;
  private static String authToken;
  private static String host = "http://" + getStargateHost();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final OkHttpClient client =
      new OkHttpClient().newBuilder().readTimeout(3, TimeUnit.MINUTES).build();
  private static final DocsHttpClient http =
      new DocsHttpClient(host, authToken, client, objectMapper);

  public CollectionTest(ClusterConnectionInfo backend) {
    super(backend);
  }

  @BeforeEach
  public void setup(ClusterConnectionInfo cluster) throws IOException {
    keyspace = "ks_docs_" + System.currentTimeMillis();
    session =
        CqlSession.builder()
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(5))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT,
                        Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(180))
                    .withDuration(
                        DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofSeconds(180))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(getStargateHost(), 9043))
            .withLocalDatacenter(cluster.datacenter())
            .build();

    assertThat(
            session
                .execute(
                    String.format(
                        "create keyspace if not exists %s WITH replication = "
                            + "{'class': 'SimpleStrategy', 'replication_factor': 1 }",
                        keyspace))
                .wasApplied())
        .isTrue();

    initAuth();
  }

  private void initAuth() throws IOException {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    RequestBody requestBody =
        RequestBody.create(
            MediaType.parse("application/json"),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")));

    Request request =
        new Request.Builder()
            .url(String.format("%s:8081/v1/auth/token/generate", host))
            .post(requestBody)
            .addHeader("X-Cassandra-Request-Id", "foo")
            .build();
    Response response = client.newCall(request).execute();
    ResponseBody body = response.body();

    assertThat(body).isNotNull();
    AuthTokenResponse authTokenResponse =
        objectMapper.readValue(body.string(), AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }

  @Test
  public void testIt() throws IOException {}
}
