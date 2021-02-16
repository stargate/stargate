/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it.http.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class GraphqlTestBase extends BaseOsgiIntegrationTest {
  protected static String host;
  protected static StargateConnectionInfo stargate;
  protected static String authToken;
  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  protected static CqlSession session;

  @BeforeAll
  public static void setupBeforeAll(StargateConnectionInfo stargateInfo) throws Exception {
    stargate = stargateInfo;
    host = "http://" + stargateInfo.seedAddress();
    createSession();
    initAuth();
  }

  private static void createSession() {
    session =
        CqlSession.builder()
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .withDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL, Duration.ofSeconds(1))
                    .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMinutes(3))
                    .withDuration(
                        DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofMinutes(3))
                    .withDuration(
                        DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT, Duration.ofMinutes(3))
                    .build())
            .withAuthCredentials("cassandra", "cassandra")
            .addContactPoint(new InetSocketAddress(stargate.seedAddress(), 9043))
            .withLocalDatacenter(stargate.datacenter())
            .build();
  }

  @AfterAll
  public static void teardown() {
    if (session != null) {
      session.close();
    }
  }

  private static void initAuth() throws IOException {
    OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.post(
            "",
            String.format("http://%s:8081/v1/auth/token/generate", stargate.seedAddress()),
            OBJECT_MAPPER.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = OBJECT_MAPPER.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }

  protected static OkHttpClient getHttpClient() {
    return getHttpClient(authToken);
  }

  public static OkHttpClient getHttpClient(String token) {
    return new OkHttpClient.Builder()
        .connectTimeout(Duration.ofMinutes(3))
        .callTimeout(Duration.ofMinutes(3))
        .readTimeout(Duration.ofMinutes(3))
        .writeTimeout(Duration.ofMinutes(3))
        .addInterceptor(
            chain ->
                chain.proceed(
                    chain.request().newBuilder().addHeader("X-Cassandra-Token", token).build()))
        .build();
  }

  protected static Map<String, Object> executeGraphqlAdminQuery(
      String query, String operationType) {
    Map<String, Object> response = getGraphqlResponse(query, operationType);
    assertThat(response).isNotNull();
    assertThat(response.get("errors")).isNull();
    @SuppressWarnings("unchecked")
    Map<String, Object> data = (Map<String, Object>) response.get("data");
    return data;
  }

  private static Map<String, Object> getGraphqlResponse(String query, String operationType) {
    try {
      OkHttpClient okHttpClient = getHttpClient();
      String url = String.format("%s:8080/graphqlv2/admin", host);
      Map<String, Object> jsonBody = new HashMap<>();
      jsonBody.put("query", query);
      jsonBody.put("operationType", operationType);

      MediaType JSON = MediaType.parse("application/json; charset=utf-8");
      okhttp3.Response response =
          okHttpClient
              .newCall(
                  new Request.Builder()
                      .post(RequestBody.create(JSON, OBJECT_MAPPER.writeValueAsString(jsonBody)))
                      .url(url)
                      .build())
              .execute();
      assertThat(response.body()).isNotNull();
      String bodyString = response.body().string();
      assertThat(response.code())
          .as("Unexpected error %d: %s", response.code(), bodyString)
          .isEqualTo(HttpStatus.SC_OK);
      @SuppressWarnings("unchecked")
      Map<String, Object> graphqlResponse = OBJECT_MAPPER.readValue(bodyString, Map.class);
      return graphqlResponse;
    } catch (IOException e) {
      fail("Unexpected error while sending POST request", e);
      return null; // never reached
    }
  }
}
