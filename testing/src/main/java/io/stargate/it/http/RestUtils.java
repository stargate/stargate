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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.http.models.Credentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import javax.validation.constraints.NotNull;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestUtils {
  private static final Logger logger = LoggerFactory.getLogger(RestUtils.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @NotNull
  public static OkHttpClient client() {
    return new OkHttpClient()
        .newBuilder()
        .readTimeout(Duration.ofMinutes(3))
        .writeTimeout(Duration.ofMinutes(3))
        .build();
  }

  public static String get(
      String authHeaderName, String authToken, String path, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    Request request;
    if (authToken != null) {
      request = new Request.Builder().url(path).get().addHeader(authHeaderName, authToken).build();
    } else {
      request = new Request.Builder().url(path).get().build();
    }

    logger.info(request.toString());

    final Response response;
    try {
      response = client.newCall(request).execute();
    } catch (SocketTimeoutException e) {
      throw new IOException(
          String.format(
              "Timeout (%s) for GET request from URL '%s': service not running?",
              request.url(), e.getClass().getName()));
    }
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String get(String authToken, String path, int expectedStatusCode)
      throws IOException {
    return get("X-Cassandra-Token", authToken, path, expectedStatusCode);
  }

  public static String postWithHeader(
      Headers headers, String path, String requestBody, int expectedStatusCode) throws IOException {
    OkHttpClient client = client();

    RequestBody rb =
        null != requestBody
            ? RequestBody.create(MediaType.parse("application/json"), requestBody)
            : RequestBody.create(null, new byte[] {});
    Request.Builder requestBuilder = new Request.Builder().url(path).post(rb);
    if (headers != null) {
      requestBuilder.headers(headers);
    }

    Request request = requestBuilder.build();

    logger.info(request.toString());

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String post(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    return postWithHeader(
        authToken == null
            ? null
            : new Headers.Builder().add("X-Cassandra-Token", authToken).build(),
        path,
        requestBody,
        expectedStatusCode);
  }

  public static String generateJwt(
      String path, String username, String password, String clientId, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    RequestBody requestBody =
        RequestBody.create(
            MediaType.parse("application/x-www-form-urlencoded"),
            String.format(
                "username=%s&password=%s&grant_type=password&client_id=%s",
                username, password, clientId));

    Request request = new Request.Builder().url(path).post(requestBody).build();

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static Response postRaw(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    Request request;
    if (authToken != null) {
      request =
          new Request.Builder()
              .url(path)
              .post(RequestBody.create(MediaType.parse("application/json"), requestBody))
              .addHeader("X-Cassandra-Token", authToken)
              .build();
    } else {
      request =
          new Request.Builder()
              .url(path)
              .post(RequestBody.create(MediaType.parse("application/json"), requestBody))
              .build();
    }

    logger.info(request.toString());

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    return response;
  }

  public static String put(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    Request request;
    if (authToken != null) {
      request =
          new Request.Builder()
              .url(path)
              .put(RequestBody.create(MediaType.parse("application/json"), requestBody))
              .addHeader("X-Cassandra-Token", authToken)
              .build();
    } else {
      request =
          new Request.Builder()
              .url(path)
              .put(RequestBody.create(MediaType.parse("application/json"), requestBody))
              .build();
    }

    logger.info(request.toString());

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String putForm(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    Request request;
    if (authToken != null) {
      request =
          new Request.Builder()
              .url(path)
              .put(
                  RequestBody.create(
                      MediaType.parse("application/x-www-form-urlencoded"), requestBody))
              .addHeader("X-Cassandra-Token", authToken)
              .build();
    } else {
      request =
          new Request.Builder()
              .url(path)
              .put(
                  RequestBody.create(
                      MediaType.parse("application/x-www-form-urlencoded"), requestBody))
              .build();
    }

    logger.info(request.toString());

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String patch(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    Request request;
    if (authToken != null) {
      request =
          new Request.Builder()
              .url(path)
              .patch(RequestBody.create(MediaType.parse("application/json"), requestBody))
              .addHeader("X-Cassandra-Token", authToken)
              .build();
    } else {
      request =
          new Request.Builder()
              .url(path)
              .patch(RequestBody.create(MediaType.parse("application/json"), requestBody))
              .build();
    }

    logger.info(request.toString());

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String delete(String authToken, String path, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = client();

    Request request;
    if (authToken != null) {
      request =
          new Request.Builder()
              .url(path)
              .delete()
              .addHeader("X-Cassandra-Token", authToken)
              .build();
    } else {
      request = new Request.Builder().url(path).delete().build();
    }

    logger.info(request.toString());

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    return response.body() != null ? response.body().string() : null;
  }

  public static void assertStatusCode(Response response, int statusCode) throws IOException {
    try {
      assertThat(response.code()).isEqualTo(statusCode);
    } catch (AssertionError e) {
      if (response.body() != null) {
        logger.error(response.body().string());
      }
      throw e;
    }
  }

  /** Gets the Stargate auth token using the given host's Auth API. */
  public static String getAuthToken(String host) {
    try {
      OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

      String body =
          RestUtils.post(
              "",
              String.format("http://%s:8081/v1/auth/token/generate", host),
              OBJECT_MAPPER.writeValueAsString(new Credentials("cassandra", "cassandra")),
              HttpStatus.SC_CREATED);

      AuthTokenResponse authTokenResponse = OBJECT_MAPPER.readValue(body, AuthTokenResponse.class);
      String authToken = authTokenResponse.getAuthToken();
      assertThat(authToken).isNotNull();
      return authToken;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
