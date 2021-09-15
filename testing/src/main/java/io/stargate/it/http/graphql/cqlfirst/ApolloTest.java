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
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.ApolloQueryCall;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.example.graphql.client.betterbotz.atomic.ProductsAndOrdersMutation;
import com.example.graphql.client.betterbotz.orders.GetOrdersByValueQuery;
import com.example.graphql.client.betterbotz.products.DeleteProductsMutation;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery.Products;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery.Value;
import com.example.graphql.client.betterbotz.products.InsertProductsMutation;
import com.example.graphql.client.betterbotz.products.UpdateProductsMutation;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import net.jcip.annotations.NotThreadSafe;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers the CQL-first API using the apollo-runtime client library.
 *
 * <p>Please do not add new test methods here. We are trying to limit usage of apollo-runtime to
 * only a few tests because it requires too much boilerplate. Instead, create new test classes and
 * use {@link CqlFirstClient} (see {@link SelectTest}, {@link InsertTest}, etc).
 *
 * <p>To update the existing methods here:
 *
 * <ul>
 *   <li>If the schema has changed, update `src/main/graphql/betterbotz/schema.json`. You can use
 *       the query in `src/main/resources/introspection.graphql` (paste it into the graphql
 *       playground at ${STARGATE_HOST}:8080/playground).
 *   <li>If there are new operations, create corresponding descriptors in
 *       `src/main/graphql/betterbotz`.
 *   <li>Run the apollo-client-maven-plugin, which reads the descriptors and generates the
 *       corresponding Java types: `mvn generate-sources` (an IDE rebuild should also work). You can
 *       see generated code in `target/generated-sources/graphql-client`.
 * </ul>
 */
@NotThreadSafe
public class ApolloTest extends ApolloTestBase {

  @Test
  public void getOrdersByValue() throws ExecutionException, InterruptedException {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    OrdersInput ordersInput = OrdersInput.builder().prodName("Medium Lift Arms").build();

    GetOrdersByValueQuery query = GetOrdersByValueQuery.builder().value(ordersInput).build();

    CompletableFuture<GetOrdersByValueQuery.Data> future = new CompletableFuture<>();
    ApolloQueryCall<Optional<GetOrdersByValueQuery.Data>> observable = client.query(query);
    observable.enqueue(queryCallback(future));

    GetOrdersByValueQuery.Data result = future.get();
    observable.cancel();

    assertThat(result.getOrders()).isPresent();

    GetOrdersByValueQuery.Orders orders = result.getOrders().get();

    assertThat(orders.getValues()).isPresent();
    List<GetOrdersByValueQuery.Value> valuesList = orders.getValues().get();

    GetOrdersByValueQuery.Value value = valuesList.get(0);
    assertThat(value.getId()).hasValue("792d0a56-bb46-4bc2-bc41-5f4a94a83da9");
    assertThat(value.getProdId()).hasValue("31047029-2175-43ce-9fdd-b3d568b19bb2");
    assertThat(value.getProdName()).hasValue("Medium Lift Arms");
    assertThat(value.getCustomerName()).hasValue("Janice Evernathy");
    assertThat(value.getAddress()).hasValue("2101 Everplace Ave 3116");
    assertThat(value.getDescription()).hasValue("Ordering some more arms for my construction bot.");
    assertThat(value.getPrice()).hasValue("3199.99");
    assertThat(value.getSellPrice()).hasValue("3119.99");
  }

  @Test
  public void insertProducts() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    String productId = UUID.randomUUID().toString();
    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    InsertProductsMutation.InsertProducts result = insertProduct(client, input);
    assertThat(result.getApplied()).hasValue(true);
    assertThat(result.getValue())
        .hasValueSatisfying(
            product -> {
              assertThat(product.getId()).hasValue(productId);
              assertThat(product.getName()).hasValue(input.name());
              assertThat(product.getPrice()).hasValue(input.price());
              assertThat(product.getCreated()).hasValue(input.created());
              assertThat(product.getDescription()).hasValue(input.description());
            });

    GetProductsWithFilterQuery.Value product = getProduct(client, productId);

    assertThat(product.getId()).hasValue(productId);
    assertThat(product.getName()).hasValue(input.name());
    assertThat(product.getPrice()).hasValue(input.price());
    assertThat(product.getCreated()).hasValue(input.created());
    assertThat(product.getDescription()).hasValue(input.description());
  }

  private InsertProductsMutation.InsertProducts insertProduct(
      ApolloClient client, ProductsInput input) {
    InsertProductsMutation mutation = InsertProductsMutation.builder().value(input).build();
    InsertProductsMutation.Data result = getObservable(client.mutate(mutation));
    assertThat(result.getInsertProducts()).isPresent();
    return result.getInsertProducts().get();
  }

  @Test
  public void updateProducts() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    String productId = UUID.randomUUID().toString();
    ProductsInput insertInput =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, insertInput);

    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name(insertInput.name())
            .price(insertInput.price())
            .created(insertInput.created())
            .description("Normal legs but shiny. Now available in different colors")
            .build();

    UpdateProductsMutation mutation = UpdateProductsMutation.builder().value(input).build();
    UpdateProductsMutation.Data result = getObservable(client.mutate(mutation));
    assertThat(result.getUpdateProducts())
        .hasValueSatisfying(
            updateProducts -> {
              assertThat(updateProducts.getApplied()).hasValue(true);
              assertThat(updateProducts.getValue())
                  .hasValueSatisfying(
                      product -> {
                        assertThat(product.getId()).hasValue(productId);
                        assertThat(product.getName()).hasValue(input.name());
                        assertThat(product.getPrice()).hasValue(input.price());
                        assertThat(product.getCreated()).hasValue(input.created());
                        assertThat(product.getDescription()).hasValue(input.description());
                      });
            });

    GetProductsWithFilterQuery.Value product = getProduct(client, productId);

    assertThat(product.getId()).hasValue(productId);
    assertThat(product.getName()).hasValue(input.name());
    assertThat(product.getPrice()).hasValue(input.price());
    assertThat(product.getCreated()).hasValue(input.created());
    assertThat(product.getDescription()).hasValue(input.description());
  }

  @Test
  public void updateProductsMissingIfExistsTrue() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    String productId = UUID.randomUUID().toString();
    ProductsInput input =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    UpdateProductsMutation mutation =
        UpdateProductsMutation.builder().value(input).ifExists(true).build();
    UpdateProductsMutation.Data result = getObservable(client.mutate(mutation));

    assertThat(result.getUpdateProducts())
        .hasValueSatisfying(
            products -> {
              assertThat(products.getApplied()).hasValue(false);
              assertThat(products.getValue())
                  .hasValueSatisfying(
                      value -> {
                        assertThat(value.getId()).isEmpty();
                        assertThat(value.getName()).isEmpty();
                        assertThat(value.getPrice()).isEmpty();
                        assertThat(value.getCreated()).isEmpty();
                        assertThat(value.getDescription()).isEmpty();
                      });
            });
  }

  @Test
  public void deleteProducts() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    String productId = UUID.randomUUID().toString();
    ProductsInput insertInput =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, insertInput);

    DeleteProductsMutation mutation =
        DeleteProductsMutation.builder()
            .value(ProductsInput.builder().id(productId).build())
            .build();

    DeleteProductsMutation.Data result = getObservable(client.mutate(mutation));

    assertThat(result.getDeleteProducts())
        .hasValueSatisfying(
            deleteProducts -> {
              assertThat(deleteProducts.getApplied()).hasValue(true);
              assertThat(deleteProducts.getValue())
                  .hasValueSatisfying(
                      product -> {
                        assertThat(product.getId()).hasValue(productId);
                        assertThat(product.getName()).isEmpty();
                        assertThat(product.getPrice()).isEmpty();
                        assertThat(product.getCreated()).isEmpty();
                        assertThat(product.getDescription()).isEmpty();
                      });
            });

    List<Value> remainingProductValues = getProductValues(client, productId);
    assertThat(remainingProductValues).isEmpty();
  }

  @Test
  public void deleteProductsIfExistsTrue() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    String productId = UUID.randomUUID().toString();
    ProductsInput insertInput =
        ProductsInput.builder()
            .id(productId)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();

    insertProduct(client, insertInput);

    ProductsInput deleteInput =
        ProductsInput.builder()
            .id(productId)
            .name(insertInput.name())
            .price(insertInput.price())
            .created(insertInput.created())
            .build();
    DeleteProductsMutation mutation =
        DeleteProductsMutation.builder().value(deleteInput).ifExists(true).build();
    DeleteProductsMutation.Data result = getObservable(client.mutate(mutation));

    assertThat(result.getDeleteProducts())
        .hasValueSatisfying(
            deleteProducts -> {
              assertThat(deleteProducts.getApplied()).hasValue(true);
            });
  }

  @Test
  @DisplayName("Should execute multiple mutations with atomic directive")
  public void shouldSupportMultipleMutationsWithAtomicDirective() {
    UUID id = UUID.randomUUID();
    String productName = "prod " + id;
    String customer = "cust " + id;
    String price = "123";
    String description = "desc " + id;

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    ProductsAndOrdersMutation mutation =
        ProductsAndOrdersMutation.builder()
            .productValue(
                ProductsInput.builder()
                    .id(id.toString())
                    .prodName(productName)
                    .price(price)
                    .name(productName)
                    .customerName(customer)
                    .created(now())
                    .description(description)
                    .build())
            .orderValue(
                OrdersInput.builder()
                    .prodName(productName)
                    .customerName(customer)
                    .price(price)
                    .description(description)
                    .build())
            .build();

    getObservable(client.mutate(mutation));

    assertThat(
            session
                .execute(SimpleStatement.newInstance("SELECT * FROM \"Products\" WHERE id = ?", id))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"prodName\""), r -> r.getString("description"))
        .containsExactly(productName, description);

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM \"Orders\" WHERE \"prodName\" = ?", productName))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"customerName\""), r -> r.getString("description"))
        .containsExactly(customer, description);
  }

  @Test
  public void invalidTypeMappingReturnsErrorResponse() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    // Expected UUID format
    GraphQLTestException ex =
        catchThrowableOfType(() -> getProduct(client, "zzz"), GraphQLTestException.class);
    assertThat(ex.errors).hasSize(1);
    assertThat(ex.errors.get(0).getMessage()).contains("Invalid UUID string");
  }

  @Test
  public void queryWithPaging() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);

    for (String name : Arrays.asList("a", "b", "c")) {
      insertProduct(
          client,
          ProductsInput.builder()
              .id(UUID.randomUUID().toString())
              .name(name)
              .price("1.0")
              .created(now())
              .build());
    }

    List<String> names = new ArrayList<>();

    Optional<Products> products = Optional.empty();
    do {
      products = getProducts(client, 1, products.flatMap(r -> r.getPageState()));
      products.ifPresent(
          p -> {
            p.getValues()
                .ifPresent(
                    values -> {
                      for (Value value : values) {
                        value.getName().ifPresent(names::add);
                      }
                    });
          });
    } while (products
        .map(p -> p.getValues().map(v -> !v.isEmpty()).orElse(false))
        .orElse(false)); // Continue if there are still values

    assertThat(names).containsExactlyInAnyOrder("a", "b", "c");
  }
}
