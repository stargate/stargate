package io.stargate.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.AuthProviderResponse;
import io.stargate.it.http.models.KeycloakCredential;
import io.stargate.it.http.models.KeycloakUser;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.Headers;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

public class KeycloakContainer {

  private static final Logger logger = LoggerFactory.getLogger(KeycloakContainer.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private String keycloakHost;
  private GenericContainer<?> keycloakContainer;

  public void initKeycloakContainer() throws IOException {
    int keycloakPort = 4444;
    keycloakContainer =
        new GenericContainer("quay.io/keycloak/keycloak:11.0.2")
            .withExposedPorts(keycloakPort)
            .withEnv("KEYCLOAK_USER", "admin")
            .withEnv("KEYCLOAK_PASSWORD", "admin")
            .withEnv("KEYCLOAK_IMPORT", "/tmp/realm.json")
            .withClasspathResourceMapping(
                "stargate-realm.json", "/tmp/realm.json", BindMode.READ_ONLY)
            .withCommand("-Djboss.http.port=" + keycloakPort)
            .waitingFor(
                Wait.forHttp("/auth/realms/master").withStartupTimeout(Duration.ofMinutes(2)));

    keycloakContainer.start();

    Slf4jLogConsumer logConsumer = new Slf4jLogConsumer(logger).withPrefix("keycloak");
    Map<String, String> mdcCopy = MDC.getCopyOfContextMap();
    if (mdcCopy != null) {
      logConsumer.withMdc(mdcCopy);
    }

    keycloakContainer.followOutput(logConsumer);

    keycloakHost =
        "http://"
            + keycloakContainer.getContainerIpAddress()
            + ":"
            + keycloakContainer.getMappedPort(keycloakPort);

    setupKeycloakUsers();
  }

  private void setupKeycloakUsers() throws IOException {
    String body =
        RestUtils.generateJwt(
            keycloakHost + "/auth/realms/master/protocol/openid-connect/token",
            "admin",
            "admin",
            "admin-cli",
            HttpStatus.SC_OK);

    AuthProviderResponse authTokenResponse =
        objectMapper.readValue(body, AuthProviderResponse.class);
    String adminAuthToken = authTokenResponse.getAccessToken();
    assertThat(adminAuthToken).isNotNull();

    KeycloakCredential keycloakCredential =
        new KeycloakCredential("password", "testuser1", "false");
    Map<String, List<String>> attributes = new HashMap<>();
    attributes.put("userid", Collections.singletonList("9876"));
    attributes.put("role", Collections.singletonList("web_user"));
    KeycloakUser keycloakUser =
        new KeycloakUser(
            "testuser1", true, true, attributes, Collections.singletonList(keycloakCredential));

    RestUtils.postWithHeader(
        new Headers.Builder().add("Authorization", "bearer " + adminAuthToken).build(),
        keycloakHost + "/auth/admin/realms/stargate/users",
        objectMapper.writeValueAsString(keycloakUser),
        HttpStatus.SC_CREATED);
  }

  public String generateJWT() throws IOException {
    String body =
        RestUtils.generateJwt(
            keycloakHost + "/auth/realms/stargate/protocol/openid-connect/token",
            "testuser1",
            "testuser1",
            "user-service",
            HttpStatus.SC_OK);

    AuthProviderResponse authTokenResponse =
        objectMapper.readValue(body, AuthProviderResponse.class);
    String authToken = authTokenResponse.getAccessToken();
    assertThat(authToken).isNotNull();

    return authToken;
  }

  public void stop() {
    keycloakContainer.stop();
  }

  public String host() {
    return keycloakHost;
  }
}
