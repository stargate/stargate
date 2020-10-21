package io.stargate.it.http.docsapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import okhttp3.*;

public class DocsHttpClient {
  private String host;
  private String authToken;
  private OkHttpClient client;
  private ObjectMapper objectMapper;

  public DocsHttpClient(String host, String authToken, OkHttpClient client, ObjectMapper mapper) {
    this.host = host;
    this.authToken = authToken;
    this.client = client;
    this.objectMapper = mapper;
  }

  public Response get(String path) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .get()
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  public Response post(String path, Object arg) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .post(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  public Response put(String path, Object arg) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .put(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  public Response patch(String path, Object arg) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .patch(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)))
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  public Response delete(String path) throws IOException {
    Request request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .delete()
            .addHeader("X-Cassandra-Token", authToken)
            .build();

    return client.newCall(request).execute();
  }

  public Response get(String path, String token) throws IOException {
    Request.Builder request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .get();

    if (token != null) {
      request = request.addHeader("X-Cassandra-Token", token);
    }

    return client.newCall(request.build()).execute();
  }

  public Response post(String path, Object arg, String token) throws IOException {
    Request.Builder request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .post(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)));

    if (token != null) {
      request = request.addHeader("X-Cassandra-Token", token);
    }

    return client.newCall(request.build()).execute();
  }

  public Response put(String path, Object arg, String token) throws IOException {
    Request.Builder request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .put(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)));

    if (token != null) {
      request = request.addHeader("X-Cassandra-Token", token);
    }

    return client.newCall(request.build()).execute();
  }

  public Response patch(String path, Object arg, String token) throws IOException {
    Request.Builder request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .patch(
                RequestBody.create(
                    MediaType.parse("application/json"), objectMapper.writeValueAsString(arg)));

    if (token != null) {
      request = request.addHeader("X-Cassandra-Token", token);
    }

    return client.newCall(request.build()).execute();
  }

  public Response delete(String path, String token) throws IOException {
    Request.Builder request =
        new Request.Builder()
            .url(String.format("%s:8082%s%s", host, path.startsWith("/") ? "" : "/", path))
            .delete();

    if (token != null) {
      request = request.addHeader("X-Cassandra-Token", token);
    }

    return client.newCall(request.build()).execute();
  }
}
