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
package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthApiError;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.auth.model.Credentials;
import io.stargate.auth.model.Secret;
import io.stargate.auth.model.UsernameCredentials;
import io.stargate.it.BaseIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.Response;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@NotThreadSafe
public class AuthApiTest extends BaseIntegrationTest {

  private String host;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  @BeforeEach
  public void setup(TestInfo testInfo, StargateConnectionInfo cluster) throws IOException {
    host = "http://" + cluster.seedAddress();
  }

  @Test
  public void authTokenGenerate() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    assertThat(authTokenResponse.getAuthToken()).isNotNull();
  }

  @Test
  public void authTokenGenerate_BadUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("bad_username", "cassandra")),
            HttpStatus.SC_UNAUTHORIZED);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username bad_username and/or password are incorrect");
  }

  @Test
  public void authTokenGenerate_BadPassword() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("cassandra", "bad_password")),
            HttpStatus.SC_UNAUTHORIZED);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username cassandra and/or password are incorrect");
  }

  @Test
  public void authTokenGenerate_MissingSecret() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            "",
            HttpStatus.SC_BAD_REQUEST);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription()).isEqualTo("Must provide a body to the request");
  }

  @Test
  public void authTokenGenerateCheckCacheHeader() throws IOException {
    Response response =
        RestUtils.postRaw(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    assertThat(response.header("Cache-Control"))
        .isEqualTo("no-transform, max-age=1790, s-maxage=1790");
  }

  @Test
  public void authTokenGenerateCheckHeaderBadCreds() throws IOException {
    Response response =
        RestUtils.postRaw(
            "",
            String.format("%s:8081/v1/auth/token/generate", host),
            objectMapper.writeValueAsString(new Secret("cassandra", "bad_password")),
            HttpStatus.SC_UNAUTHORIZED);

    assertThat(response.header("Cache-Control"))
        .isEqualTo("no-store, no-transform, max-age=0, s-maxage=0");
  }

  @Test
  public void auth() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    assertThat(authTokenResponse.getAuthToken()).isNotNull();
  }

  @Test
  public void auth_BadUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("bad_username", "cassandra")),
            HttpStatus.SC_UNAUTHORIZED);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username bad_username and/or password are incorrect");
  }

  @Test
  public void auth_BadPassword() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "bad_password")),
            HttpStatus.SC_UNAUTHORIZED);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription())
        .isEqualTo(
            "Failed to create token: Provided username cassandra and/or password are incorrect");
  }

  @Test
  public void auth_MissingCredentials() throws IOException {
    String body =
        RestUtils.post("", String.format("%s:8081/v1/auth", host), "", HttpStatus.SC_BAD_REQUEST);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription()).isEqualTo("Must provide a body to the request");
  }

  @Test
  public void authCheckCacheHeader() throws IOException {
    Response response =
        RestUtils.postRaw(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    assertThat(response.header("Cache-Control"))
        .isEqualTo("no-transform, max-age=1790, s-maxage=1790");
  }

  @Test
  public void authCheckHeaderBadCreds() throws IOException {
    Response response =
        RestUtils.postRaw(
            "",
            String.format("%s:8081/v1/auth", host),
            objectMapper.writeValueAsString(new Credentials("bad_username", "cassandra")),
            HttpStatus.SC_UNAUTHORIZED);

    assertThat(response.header("Cache-Control"))
        .isEqualTo("no-store, no-transform, max-age=0, s-maxage=0");
  }

  @Test
  public void authUsernameToken() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            objectMapper.writeValueAsString(new UsernameCredentials("cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    assertThat(authTokenResponse.getAuthToken()).isNotNull();
  }

  @Test
  public void authUsernameToken_BadUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            objectMapper.writeValueAsString(new UsernameCredentials("bad_user_name")),
            HttpStatus.SC_UNAUTHORIZED);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription())
        .isEqualTo("Failed to create token: Provided username bad_user_name is incorrect");
  }

  @Test
  public void authUsernameToken_MissingUsername() throws IOException {
    String body =
        RestUtils.post(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            "",
            HttpStatus.SC_BAD_REQUEST);

    AuthApiError error = objectMapper.readValue(body, AuthApiError.class);
    assertThat(error.getDescription()).isEqualTo("Must provide a body to the request");
  }

  @Test
  public void authUsernameTokenCheckHeader() throws IOException {
    Response response =
        RestUtils.postRaw(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            objectMapper.writeValueAsString(new UsernameCredentials("cassandra")),
            HttpStatus.SC_CREATED);

    assertThat(response.header("Cache-Control"))
        .isEqualTo("no-transform, max-age=1790, s-maxage=1790");
  }

  @Test
  public void authUsernameToken_CheckHeaderBadUsername() throws IOException {
    Response response =
        RestUtils.postRaw(
            "",
            String.format("%s:8081/v1/admin/auth/usernametoken", host),
            objectMapper.writeValueAsString(new UsernameCredentials("bad_user_name")),
            HttpStatus.SC_UNAUTHORIZED);

    assertThat(response.header("Cache-Control"))
        .isEqualTo("no-store, no-transform, max-age=0, s-maxage=0");
  }
}
