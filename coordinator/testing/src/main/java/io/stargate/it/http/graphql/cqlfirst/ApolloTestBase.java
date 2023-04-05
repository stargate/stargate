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
package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.apollographql.apollo.ApolloCall;
import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloMutationCall;
import com.apollographql.apollo.api.CustomTypeAdapter;
import com.apollographql.apollo.api.CustomTypeValue;
import com.apollographql.apollo.api.Error;
import com.apollographql.apollo.api.Mutation;
import com.apollographql.apollo.api.Operation;
import com.apollographql.apollo.api.Response;
import com.apollographql.apollo.exception.ApolloException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.example.graphql.client.betterbotz.products.DeleteProductsMutation;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery;
import com.example.graphql.client.betterbotz.type.CustomType;
import com.example.graphql.client.betterbotz.type.ProductsFilterInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import com.example.graphql.client.betterbotz.type.QueryConsistency;
import com.example.graphql.client.betterbotz.type.QueryOptions;
import com.example.graphql.client.betterbotz.type.UuidFilterInput;
import io.stargate.it.driver.TestKeyspace;
import io.stargate.it.http.RestUtils;
import io.stargate.it.storage.StargateConnectionInfo;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import okhttp3.OkHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for GraphQL tests that use the apollo-runtime client library.
 *
 * <p>Note that we are trying to limit usage of that library in our tests. Do not subclass this in
 * new tests; instead, use {@link CqlFirstClient} (see {@link SelectTest}, {@link InsertTest}, etc).
 *
 * <p>Eventually, {@link ApolloTest} should be the only subclass, and we might merge this into it.
 */
public class ApolloTestBase extends BetterbotzTestBase {

  protected static final Logger logger = LoggerFactory.getLogger(ApolloTest.class);

  protected static CqlSession session;
  protected static String authToken;
  protected static StargateConnectionInfo stargate;
  protected static String keyspace;

  @BeforeAll
  public static void setup(
      StargateConnectionInfo stargateInfo,
      CqlSession session,
      @TestKeyspace CqlIdentifier keyspaceId)
      throws Exception {
    stargate = stargateInfo;
    ApolloTest.session = session;
    keyspace = keyspaceId.asInternal();

    authToken = RestUtils.getAuthToken(stargate.seedAddress());
  }

  @AfterEach
  public void cleanUpProducts() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    getProducts(client, 100, Optional.empty())
        .flatMap(GetProductsWithFilterQuery.Products::getValues)
        .ifPresent(
            products ->
                products.forEach(p -> p.getId().ifPresent(id -> cleanupProduct(client, id))));
  }

  protected static Optional<GetProductsWithFilterQuery.Products> getProducts(
      ApolloClient client, int pageSize, Optional<String> pageState) {
    ProductsFilterInput filterInput = ProductsFilterInput.builder().build();

    QueryOptions.Builder optionsBuilder =
        QueryOptions.builder().pageSize(pageSize).consistency(QueryConsistency.LOCAL_QUORUM);

    pageState.ifPresent(optionsBuilder::pageState);
    QueryOptions options = optionsBuilder.build();

    GetProductsWithFilterQuery query =
        GetProductsWithFilterQuery.builder().filter(filterInput).options(options).build();

    GetProductsWithFilterQuery.Data result = getObservable(client.query(query));

    assertThat(result.getProducts())
        .hasValueSatisfying(
            products -> {
              assertThat(products.getValues())
                  .hasValueSatisfying(
                      values -> {
                        assertThat(values).hasSizeLessThanOrEqualTo(pageSize);
                      });
            });

    return result.getProducts();
  }

  private DeleteProductsMutation.Data cleanupProduct(ApolloClient client, Object productId) {
    DeleteProductsMutation mutation =
        DeleteProductsMutation.builder()
            .value(ProductsInput.builder().id(productId).build())
            .build();

    DeleteProductsMutation.Data result = getObservable(client.mutate(mutation));
    return result;
  }

  protected GetProductsWithFilterQuery.Value getProduct(ApolloClient client, String productId) {
    List<GetProductsWithFilterQuery.Value> valuesList = getProductValues(client, productId);
    return valuesList.get(0);
  }

  protected List<GetProductsWithFilterQuery.Value> getProductValues(
      ApolloClient client, String productId) {
    ProductsFilterInput filterInput =
        ProductsFilterInput.builder().id(UuidFilterInput.builder().eq(productId).build()).build();

    QueryOptions options =
        QueryOptions.builder().consistency(QueryConsistency.LOCAL_QUORUM).build();

    GetProductsWithFilterQuery query =
        GetProductsWithFilterQuery.builder().filter(filterInput).options(options).build();

    GetProductsWithFilterQuery.Data result = getObservable(client.query(query));
    assertThat(result.getProducts()).isPresent();
    GetProductsWithFilterQuery.Products products = result.getProducts().get();
    assertThat(products.getValues()).isPresent();
    return products.getValues().get();
  }

  protected static <T> T getObservable(ApolloCall<Optional<T>> observable) {
    CompletableFuture<T> future = new CompletableFuture<>();
    observable.enqueue(queryCallback(future));

    try {
      return future.get();
    } catch (ExecutionException e) {
      // Unwrap exception
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException("Unexpected exception", e);
    } catch (Exception e) {
      throw new RuntimeException("Operation could not be completed", e);
    } finally {
      observable.cancel();
    }
  }

  @SuppressWarnings("unchecked")
  private static <D extends Operation.Data, T, V extends Operation.Variables> D mutateAndGet(
      ApolloClient client, Mutation<D, T, V> mutation) {
    return getObservable((ApolloMutationCall<Optional<D>>) client.mutate(mutation));
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

  protected static <U> ApolloCall.Callback<Optional<U>> queryCallback(CompletableFuture<U> future) {
    return new ApolloCall.Callback<Optional<U>>() {
      @Override
      public void onResponse(@NotNull Response<Optional<U>> response) {
        if (response.getErrors() != null && response.getErrors().size() > 0) {
          logger.info(
              "GraphQL error found in test: {}",
              response.getErrors().stream().map(Error::getMessage).collect(Collectors.toList()));
          future.completeExceptionally(
              new GraphQLTestException("GraphQL error response", response.getErrors()));
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

  protected static class GraphQLTestException extends RuntimeException {
    protected final List<Error> errors;

    GraphQLTestException(String message, List<Error> errors) {
      super(message);
      this.errors = errors;
    }
  }

  private static Instant parseInstant(String source) {
    try {
      return TIMESTAMP_FORMAT.get().parse(source).toInstant();
    } catch (ParseException e) {
      throw new AssertionError("Unexpected error while parsing timestamp in response", e);
    }
  }

  private static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMAT =
      ThreadLocal.withInitial(
          () -> {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
            parser.setTimeZone(TimeZone.getTimeZone(ZoneId.systemDefault()));
            return parser;
          });
}
