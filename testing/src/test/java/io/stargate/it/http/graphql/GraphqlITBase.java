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

import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloMutationCall;
import com.apollographql.apollo.ApolloQueryCall;
import com.apollographql.apollo.api.CustomTypeAdapter;
import com.apollographql.apollo.api.CustomTypeValue;
import com.apollographql.apollo.api.Error;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.example.graphql.client.betterbotz.type.CustomType;
import com.example.graphql.client.schema.CreateTableMutation;
import com.example.graphql.client.schema.GetTableQuery;
import com.example.graphql.client.schema.type.ColumnInput;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.auth.model.AuthTokenResponse;
import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.http.GraphqlTest;
import io.stargate.it.http.RestUtils;
import io.stargate.it.http.models.Credentials;
import io.stargate.it.storage.StargateConnectionInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import okhttp3.OkHttpClient;
import org.apache.http.HttpStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphqlITBase extends BaseOsgiIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(GraphqlITBase.class);
  protected static String host;
  protected static StargateConnectionInfo stargate;
  protected static String authToken;
  protected static final ObjectMapper objectMapper = new ObjectMapper();
  protected static CqlSession session;

  @BeforeAll
  public static void setup(StargateConnectionInfo stargateInfo) throws Exception {
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
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    String body =
        RestUtils.post(
            "",
            String.format("http://%s:8081/v1/auth/token/generate", stargate.seedAddress()),
            objectMapper.writeValueAsString(new Credentials("cassandra", "cassandra")),
            HttpStatus.SC_CREATED);

    AuthTokenResponse authTokenResponse = objectMapper.readValue(body, AuthTokenResponse.class);
    authToken = authTokenResponse.getAuthToken();
    assertThat(authToken).isNotNull();
  }

  protected OkHttpClient getHttpClient() {
    return new OkHttpClient.Builder()
        .connectTimeout(Duration.ofMinutes(3))
        .callTimeout(Duration.ofMinutes(3))
        .readTimeout(Duration.ofMinutes(3))
        .writeTimeout(Duration.ofMinutes(3))
        .addInterceptor(
            chain ->
                chain.proceed(
                    chain.request().newBuilder().addHeader("X-Cassandra-Token", authToken).build()))
        .build();
  }

  protected ApolloClient getApolloClient(String path) {
    return ApolloClient.builder()
        .serverUrl(String.format("http://%s:8080%s", stargate.seedAddress(), path))
        .okHttpClient(getHttpClient())
        .addCustomTypeAdapter(
            CustomType.TIMESTAMP,
            new CustomTypeAdapter<Instant>() {
              @NotNull
              @Override
              public CustomTypeValue<?> encode(Instant instant) {
                return new CustomTypeValue.GraphQLString(instant.toString());
              }

              @Override
              public Instant decode(@NotNull CustomTypeValue<?> customTypeValue) {
                return parseInstant(customTypeValue.value.toString());
              }
            })
        .build();
  }

  protected static Instant parseInstant(String source) {
    try {
      return TIMESTAMP_FORMAT.get().parse(source).toInstant();
    } catch (ParseException e) {
      throw new AssertionError("Unexpected error while parsing timestamp in response", e);
    }
  }

  protected static String formatInstant(Instant instant) {
    return TIMESTAMP_FORMAT.get().format(Date.from(instant));
  }

  protected static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMAT =
      ThreadLocal.withInitial(
          () -> {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            parser.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));
            return parser;
          });

  protected GetTableQuery.Table createTable(
      ApolloClient client,
      String keyspaceName,
      String tableName,
      List<ColumnInput> partitionKeys,
      List<ColumnInput> values)
      throws ExecutionException, InterruptedException {
    return createTable(
        client,
        tableName,
        CreateTableMutation.builder()
            .keyspaceName(keyspaceName)
            .partitionKeys(partitionKeys)
            .values(values),
        keyspaceName);
  }

  public GetTableQuery.Table getTable(ApolloClient client, String keyspaceName, String tableName)
      throws ExecutionException, InterruptedException {
    GetTableQuery query =
        GetTableQuery.builder().keyspaceName(keyspaceName).tableName(tableName).build();

    CompletableFuture<GetTableQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetTableQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetTableQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getKeyspace()).isPresent();

    GetTableQuery.Keyspace keyspace = result.getKeyspace().get();
    assertThat(keyspace.getName()).isEqualTo(keyspaceName);
    assertThat(keyspace.getTable()).isPresent();

    GetTableQuery.Table table = keyspace.getTable().get();
    assertThat(table.getName()).isEqualTo(tableName);

    return table;
  }

  protected GetTableQuery.Table createTable(
      ApolloClient client,
      String tableName,
      CreateTableMutation.Builder mutationBuilder,
      String keyspace)
      throws ExecutionException, InterruptedException {

    CreateTableMutation mutation =
        mutationBuilder.keyspaceName(keyspace).tableName(tableName).build();
    CompletableFuture<CreateTableMutation.Data> future = new CompletableFuture<>();
    ApolloMutationCall<Optional<CreateTableMutation.Data>> observable = client.mutate(mutation);
    observable.enqueue(queryCallback(future));

    CreateTableMutation.Data result = future.get();
    observable.cancel();

    assertThat(result.getCreateTable()).hasValue(true);

    GetTableQuery.Table table = getTable(client, keyspace, tableName);
    assertThat(table.getName()).isEqualTo(tableName);

    return table;
  }

  protected static <U> ApolloCall.Callback<Optional<U>> queryCallback(CompletableFuture<U> future) {
    return new ApolloCall.Callback<Optional<U>>() {
      @Override
      public void onResponse(@NotNull Response<Optional<U>> response) {
        if (response.getErrors() != null && response.getErrors().size() > 0) {
          logger.info(
              "GraphQL error found in test: {}",
              response.getErrors().stream().map(Error::getMessage).collect(Collectors.toList()));
          future.completeExceptionally(
              new GraphqlTest.GraphQLTestException("GraphQL error response", response.getErrors()));
          return;
        }

        if (response.getData().isPresent()) {
          future.complete(response.getData().get());
          return;
        }

        future.completeExceptionally(
            new IllegalStateException("Unexpected empty data and errors properties"));
      }

      @Override
      public void onFailure(@NotNull ApolloException e) {
        future.completeExceptionally(e);
      }
    };
  }

  public static class GraphQLTestException extends RuntimeException {
    public final List<Error> errors;

    GraphQLTestException(String message, List<Error> errors) {
      super(message);
      this.errors = errors;
    }
  }
}
