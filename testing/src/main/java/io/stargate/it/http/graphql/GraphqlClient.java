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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.it.http.RestUtils;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.http.HttpStatus;

/**
 * A lightweight client for GraphQL tests.
 *
 * <p>Queries are passed as plain strings, and results returned as raw JSON structures.
 */
public abstract class GraphqlClient {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected Map<String, Object> getGraphqlData(String authToken, String url, String graphqlQuery) {
    Map<String, Object> response =
        getGraphqlResponse(
            Collections.singletonMap("X-Cassandra-Token", authToken),
            url,
            graphqlQuery,
            HttpStatus.SC_OK);
    assertThat(response).isNotNull();
    assertThat(response.get("errors")).isNull();
    @SuppressWarnings("unchecked")
    Map<String, Object> data = (Map<String, Object>) response.get("data");
    return data;
  }

  protected String getGraphqlError(
      String authToken, String url, String graphqlQuery, int expectedStatus) {
    List<Map<String, Object>> errors =
        getGraphqlErrors(authToken, url, graphqlQuery, expectedStatus);
    assertThat(errors).hasSize(1);
    return (String) errors.get(0).get("message");
  }

  protected List<Map<String, Object>> getGraphqlErrors(
      String authToken, String url, String graphqlQuery, int expectedStatus) {
    Map<String, Object> response =
        getGraphqlResponse(
            Collections.singletonMap("X-Cassandra-Token", authToken),
            url,
            graphqlQuery,
            expectedStatus);
    assertThat(response).isNotNull();
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> errors = (List<Map<String, Object>>) response.get("errors");
    assertThat(errors).isNotEmpty();
    return errors;
  }

  protected Map<String, Object> getGraphqlResponse(
      Map<String, String> headers, String url, String graphqlQuery, int expectedStatus) {
    try {
      OkHttpClient okHttpClient = RestUtils.client();
      Map<String, Object> formData = new HashMap<>();
      formData.put("query", graphqlQuery);

      MediaType JSON = MediaType.parse("application/json; charset=utf-8");
      Request.Builder requestBuilder =
          new Request.Builder()
              .post(RequestBody.create(JSON, OBJECT_MAPPER.writeValueAsBytes(formData)))
              .url(url);
      requestBuilder.headers(Headers.of(headers));
      okhttp3.Response response = okHttpClient.newCall(requestBuilder.build()).execute();
      assertThat(response.body()).isNotNull();
      String bodyString = response.body().string();
      assertThat(response.code())
          .as("Unexpected error %d: %s", response.code(), bodyString)
          .isEqualTo(expectedStatus);
      @SuppressWarnings("unchecked")
      Map<String, Object> graphqlResponse = OBJECT_MAPPER.readValue(bodyString, Map.class);
      return graphqlResponse;
    } catch (IOException e) {
      return fail("Unexpected error while sending POST request", e);
    }
  }

  protected String urlEncode(String parameter) {
    try {
      return URLEncoder.encode(parameter, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError(e);
    }
  }
}
