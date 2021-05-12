package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.apollographql.apollo.ApolloClient;
import com.example.graphql.client.betterbotz.async.BulkInsertProductsWithAsyncAndAtomicMutation;
import com.example.graphql.client.betterbotz.async.BulkInsertProductsWithAsyncMutation;
import com.example.graphql.client.betterbotz.async.InsertOrdersWithAsyncAndAtomicMutation;
import com.example.graphql.client.betterbotz.async.InsertOrdersWithAsyncMutation;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class AsyncDirectiveTest extends ApolloTestBase {

  @Test
  @DisplayName("Should insert using both atomic and async directives")
  public void shouldInsertWhenUsingBothAsyncAndAtomicDirectives() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersWithAsyncAndAtomicMutation mutation =
        InsertOrdersWithAsyncAndAtomicMutation.builder()
            .value(OrdersInput.builder().prodName("a").customerName("b").description("c").build())
            .build();

    InsertOrdersWithAsyncAndAtomicMutation.InsertOrders result =
        getObservable(client.mutate(mutation)).getInsertOrders().get();

    assertThat(result.getAccepted()).hasValue(true);
  }

  @Test
  @DisplayName("Should bulk insert using both atomic and async directives")
  public void shouldBulkInsertWhenUsingBothAsyncAndAtomicDirectives() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    BulkInsertProductsWithAsyncAndAtomicMutation mutation =
        BulkInsertProductsWithAsyncAndAtomicMutation.builder()
            .values(
                Arrays.asList(
                    ProductsInput.builder()
                        .id(UUID.randomUUID().toString())
                        .name("Shiny Legs")
                        .price("3199.99")
                        .created(now())
                        .description("Normal legs but shiny.")
                        .build(),
                    ProductsInput.builder()
                        .id(UUID.randomUUID().toString())
                        .name("other")
                        .price("3000.99")
                        .created(now())
                        .description("Normal legs but shiny.")
                        .build()))
            .build();

    List<BulkInsertProductsWithAsyncAndAtomicMutation.BulkInsertProduct> result =
        getObservable(client.mutate(mutation)).getBulkInsertProducts().get();

    assertThat(result.get(0).getAccepted()).hasValue(true);
    assertThat(result.get(1).getAccepted()).hasValue(true);
  }

  @Test
  @DisplayName("Should insert orders using async directive")
  public void shouldInsertOrdersUsingAsyncDirective() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersWithAsyncMutation mutation =
        InsertOrdersWithAsyncMutation.builder()
            .value(OrdersInput.builder().prodName("a").customerName("b").description("c").build())
            .build();

    InsertOrdersWithAsyncMutation.InsertOrders result =
        getObservable(client.mutate(mutation)).getInsertOrders().get();

    assertThat(result.getAccepted()).hasValue(true);
  }

  @Test
  @DisplayName("Should bulk insert products using async directive")
  public void shouldBulkInsertProductsUsingAsyncDirective() {
    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    BulkInsertProductsWithAsyncMutation mutation =
        BulkInsertProductsWithAsyncMutation.builder()
            .values(
                Arrays.asList(
                    ProductsInput.builder()
                        .id(UUID.randomUUID().toString())
                        .name("Shiny Legs")
                        .price("3199.99")
                        .created(now())
                        .description("Normal legs but shiny.")
                        .build(),
                    ProductsInput.builder()
                        .id(UUID.randomUUID().toString())
                        .name("other")
                        .price("3000.99")
                        .created(now())
                        .description("Normal legs but shiny.")
                        .build()))
            .build();

    List<BulkInsertProductsWithAsyncMutation.BulkInsertProduct> result =
        getObservable(client.mutate(mutation)).getBulkInsertProducts().get();

    assertThat(result.get(0).getAccepted()).hasValue(true);
    assertThat(result.get(1).getAccepted()).hasValue(true);
  }
}
