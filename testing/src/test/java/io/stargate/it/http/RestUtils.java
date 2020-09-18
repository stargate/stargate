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

import java.io.IOException;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestUtils {
  private static final Logger logger = LoggerFactory.getLogger(RestUtils.class);

  public static String get(String authToken, String path, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    Request request =
        new Request.Builder().url(path).get().addHeader("X-Cassandra-Token", authToken).build();

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String post(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    Request request =
        new Request.Builder()
            .url(path)
            .post(RequestBody.create(MediaType.parse("application/json"), requestBody))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String put(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    Request request =
        new Request.Builder()
            .url(path)
            .put(RequestBody.create(MediaType.parse("application/json"), requestBody))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String patch(
      String authToken, String path, String requestBody, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    Request request =
        new Request.Builder()
            .url(path)
            .patch(RequestBody.create(MediaType.parse("application/json"), requestBody))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    Response response = client.newCall(request).execute();
    assertStatusCode(response, expectedStatusCode);

    ResponseBody body = response.body();
    assertThat(body).isNotNull();

    return body.string();
  }

  public static String delete(String authToken, String path, int expectedStatusCode)
      throws IOException {
    OkHttpClient client = new OkHttpClient().newBuilder().build();

    Request request =
        new Request.Builder().url(path).delete().addHeader("X-Cassandra-Token", authToken).build();

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
}
