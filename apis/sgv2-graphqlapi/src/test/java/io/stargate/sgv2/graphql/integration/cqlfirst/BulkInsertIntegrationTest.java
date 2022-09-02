package io.stargate.sgv2.graphql.integration.cqlfirst;

import static org.assertj.core.api.Assertions.assertThat;

import com.apollographql.apollo.ApolloClient;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.example.graphql.client.betterbotz.atomic.BulkInsertProductsAndOrdersWithAtomicMutation;
import com.example.graphql.client.betterbotz.atomic.BulkInsertProductsWithAtomicMutation;
import com.example.graphql.client.betterbotz.atomic.InsertOrdersAndBulkInsertProductsWithAtomicMutation;
import com.example.graphql.client.betterbotz.products.BulkInsertProductsMutation;
import com.example.graphql.client.betterbotz.products.GetProductsWithFilterQuery;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.ProductsInput;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.stargate.sgv2.common.testresource.StargateTestResource;
import io.stargate.sgv2.graphql.integration.util.ApolloIntegrationTestBase;
import io.stargate.sgv2.graphql.integration.util.CqlFirstClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * TODO refactor this to use {@link CqlFirstClient} instead of extending {@link
 * ApolloIntegrationTestBase}.
 */
@QuarkusIntegrationTest
@QuarkusTestResource(
    value = StargateTestResource.class,
    initArgs =
        @ResourceArg(name = StargateTestResource.Options.DISABLE_FIXED_TOKEN, value = "true"))
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BulkInsertIntegrationTest extends ApolloIntegrationTestBase {

  @Test
  public void bulkInsertProducts() {
    ApolloClient client = getApolloClient("graphql/" + keyspaceId.asInternal());

    String productId1 = UUID.randomUUID().toString();
    String productId2 = UUID.randomUUID().toString();
    ProductsInput product1 =
        ProductsInput.builder()
            .id(productId1)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();
    ProductsInput product2 =
        ProductsInput.builder()
            .id(productId2)
            .name("Non-Legs")
            .price("1000.99")
            .created(now())
            .description("Non-legs.")
            .build();

    List<BulkInsertProductsMutation.BulkInsertProduct> bulkInsertedProducts =
        bulkInsertProducts(client, Arrays.asList(product1, product2));

    BulkInsertProductsMutation.BulkInsertProduct firstInsertedProduct = bulkInsertedProducts.get(0);
    BulkInsertProductsMutation.BulkInsertProduct secondInsertedProduct =
        bulkInsertedProducts.get(1);

    assertThat(firstInsertedProduct.getApplied().get()).isTrue();
    assertThat(firstInsertedProduct.getValue())
        .hasValueSatisfying(
            value -> {
              assertThat(value.getId()).hasValue(productId1);
            });

    assertThat(secondInsertedProduct.getApplied().get()).isTrue();
    assertThat(secondInsertedProduct.getValue())
        .hasValueSatisfying(
            value -> {
              assertThat(value.getId()).hasValue(productId2);
            });

    // retrieve from db
    GetProductsWithFilterQuery.Value product1Result = getProduct(client, productId1);

    assertThat(product1Result.getId()).hasValue(productId1);
    assertThat(product1Result.getName()).hasValue(product1.name());
    assertThat(product1Result.getPrice()).hasValue(product1.price());
    assertThat(product1Result.getCreated()).hasValue(product1.created());
    assertThat(product1Result.getDescription()).hasValue(product1.description());

    GetProductsWithFilterQuery.Value product2Result = getProduct(client, productId2);

    assertThat(product2Result.getId()).hasValue(productId2);
    assertThat(product2Result.getName()).hasValue(product2.name());
    assertThat(product2Result.getPrice()).hasValue(product2.price());
    assertThat(product2Result.getCreated()).hasValue(product2.created());
    assertThat(product2Result.getDescription()).hasValue(product2.description());
  }

  @Test
  public void bulkInsertProductsWithAtomic() {
    ApolloClient client = getApolloClient("graphql/" + keyspaceId.asInternal());

    String productId1 = UUID.randomUUID().toString();
    String productId2 = UUID.randomUUID().toString();
    ProductsInput product1 =
        ProductsInput.builder()
            .id(productId1)
            .name("Shiny Legs")
            .price("3199.99")
            .created(now())
            .description("Normal legs but shiny.")
            .build();
    ProductsInput product2 =
        ProductsInput.builder()
            .id(productId2)
            .name("Non-Legs")
            .price("1000.99")
            .created(now())
            .description("Non-legs.")
            .build();

    bulkInsertProductsWithAtomic(client, Arrays.asList(product1, product2));

    GetProductsWithFilterQuery.Value product1Result = getProduct(client, productId1);

    assertThat(product1Result.getId()).hasValue(productId1);
    assertThat(product1Result.getName()).hasValue(product1.name());
    assertThat(product1Result.getPrice()).hasValue(product1.price());
    assertThat(product1Result.getCreated()).hasValue(product1.created());
    assertThat(product1Result.getDescription()).hasValue(product1.description());

    GetProductsWithFilterQuery.Value product2Result = getProduct(client, productId2);

    assertThat(product2Result.getId()).hasValue(productId2);
    assertThat(product2Result.getName()).hasValue(product2.name());
    assertThat(product2Result.getPrice()).hasValue(product2.price());
    assertThat(product2Result.getCreated()).hasValue(product2.created());
    assertThat(product2Result.getDescription()).hasValue(product2.description());
  }

  @Test
  @DisplayName("Should execute multiple mutations including bulk with atomic directive")
  public void bulkInsertNProductsUsingBulkAndOrderWithAtomic() {
    String productId1 = UUID.randomUUID().toString();
    String productId2 = UUID.randomUUID().toString();
    String productName = "Shiny Legs";
    String description = "Normal legs but shiny.";
    ProductsInput product1 =
        ProductsInput.builder()
            .id(productId1)
            .name(productName)
            .price("3199.99")
            .created(now())
            .description(description)
            .build();
    ProductsInput product2 =
        ProductsInput.builder()
            .id(productId2)
            .name("Non-Legs")
            .price("1000.99")
            .created(now())
            .description("Non-legs.")
            .build();

    String customerName = "c1";
    OrdersInput order =
        OrdersInput.builder()
            .prodName(productName)
            .customerName(customerName)
            .price("3199.99")
            .description(description)
            .build();

    ApolloClient client = getApolloClient("graphql/" + keyspaceId.asInternal());
    BulkInsertProductsAndOrdersWithAtomicMutation mutation =
        BulkInsertProductsAndOrdersWithAtomicMutation.builder()
            .values(Arrays.asList(product1, product2))
            .orderValue(order)
            .build();

    List<BulkInsertProductsAndOrdersWithAtomicMutation.Product> result =
        bulkInsertProductsAndOrdersWithAtomic(client, mutation).getProducts().get();
    BulkInsertProductsAndOrdersWithAtomicMutation.Product firstInsertedProduct = result.get(0);
    BulkInsertProductsAndOrdersWithAtomicMutation.Product secondInsertedProduct = result.get(1);

    assertThat(firstInsertedProduct.getApplied().get()).isTrue();
    assertThat(firstInsertedProduct.getValue())
        .hasValueSatisfying(
            value -> {
              assertThat(value.getId()).hasValue(productId1);
            });

    assertThat(secondInsertedProduct.getApplied().get()).isTrue();
    assertThat(secondInsertedProduct.getValue())
        .hasValueSatisfying(
            value -> {
              assertThat(value.getId()).hasValue(productId2);
            });

    // retrieve from db
    GetProductsWithFilterQuery.Value product1Result = getProduct(client, productId1);

    assertThat(product1Result.getId()).hasValue(productId1);
    assertThat(product1Result.getName()).hasValue(product1.name());
    assertThat(product1Result.getPrice()).hasValue(product1.price());
    assertThat(product1Result.getCreated()).hasValue(product1.created());
    assertThat(product1Result.getDescription()).hasValue(product1.description());

    GetProductsWithFilterQuery.Value product2Result = getProduct(client, productId2);

    assertThat(product2Result.getId()).hasValue(productId2);
    assertThat(product2Result.getName()).hasValue(product2.name());
    assertThat(product2Result.getPrice()).hasValue(product2.price());
    assertThat(product2Result.getCreated()).hasValue(product2.created());
    assertThat(product2Result.getDescription()).hasValue(product2.description());

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM \"Orders\" WHERE \"prodName\" = ?", productName))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"customerName\""), r -> r.getString("description"))
        .containsExactly(customerName, description);
  }

  @Test
  @DisplayName(
      "Should execute multiple mutations including bulk with more elements than selections with atomic directive")
  public void bulkInsertMoreProductsThanSelectionsUsingBulkAndOrderWithAtomic() {
    String productName = "Shiny Legs";
    String description = "Normal legs but shiny.";
    List<ProductsInput> productsInputs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      productsInputs.add(
          ProductsInput.builder()
              .id(UUID.randomUUID().toString())
              .name(productName)
              .price("3199.99")
              .created(now())
              .description(description)
              .build());
    }

    String customerName = "c1";
    OrdersInput order =
        OrdersInput.builder()
            .prodName(productName)
            .customerName(customerName)
            .price("3199.99")
            .description(description)
            .build();

    ApolloClient client = getApolloClient("graphql/" + keyspaceId.asInternal());
    BulkInsertProductsAndOrdersWithAtomicMutation mutation =
        BulkInsertProductsAndOrdersWithAtomicMutation.builder()
            .values(productsInputs)
            .orderValue(order)
            .build();

    bulkInsertProductsAndOrdersWithAtomic(client, mutation);

    for (ProductsInput product : productsInputs) {
      GetProductsWithFilterQuery.Value product1Result = getProduct(client, (String) product.id());

      assertThat(product1Result.getId()).hasValue(product.id());
      assertThat(product1Result.getName()).hasValue(product.name());
      assertThat(product1Result.getPrice()).hasValue(product.price());
      assertThat(product1Result.getCreated()).hasValue(product.created());
      assertThat(product1Result.getDescription()).hasValue(product.description());
    }

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM \"Orders\" WHERE \"prodName\" = ?", productName))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"customerName\""), r -> r.getString("description"))
        .containsExactly(customerName, description);
  }

  @Test
  @DisplayName("Should execute normal insert and multiple bulk mutations with atomic directive")
  public void insertOrderAndBulkInsertNProductsWithAtomic() {
    String productId1 = UUID.randomUUID().toString();
    String productId2 = UUID.randomUUID().toString();
    String productName = "Shiny Legs";
    String description = "Normal legs but shiny.";
    ProductsInput product1 =
        ProductsInput.builder()
            .id(productId1)
            .name(productName)
            .price("3199.99")
            .created(now())
            .description(description)
            .build();
    ProductsInput product2 =
        ProductsInput.builder()
            .id(productId2)
            .name("Non-Legs")
            .price("1000.99")
            .created(now())
            .description("Non-legs.")
            .build();

    String customerName = "c1";
    OrdersInput order =
        OrdersInput.builder()
            .prodName(productName)
            .customerName(customerName)
            .price("3199.99")
            .description(description)
            .build();

    ApolloClient client = getApolloClient("graphql/" + keyspaceId.asInternal());
    InsertOrdersAndBulkInsertProductsWithAtomicMutation mutation =
        InsertOrdersAndBulkInsertProductsWithAtomicMutation.builder()
            .values(Arrays.asList(product1, product2))
            .orderValue(order)
            .build();

    insertOrdersAndBulkInsertProductsWthAtomic(client, mutation);

    GetProductsWithFilterQuery.Value product1Result = getProduct(client, productId1);

    assertThat(product1Result.getId()).hasValue(productId1);
    assertThat(product1Result.getName()).hasValue(product1.name());
    assertThat(product1Result.getPrice()).hasValue(product1.price());
    assertThat(product1Result.getCreated()).hasValue(product1.created());
    assertThat(product1Result.getDescription()).hasValue(product1.description());

    GetProductsWithFilterQuery.Value product2Result = getProduct(client, productId2);

    assertThat(product2Result.getId()).hasValue(productId2);
    assertThat(product2Result.getName()).hasValue(product2.name());
    assertThat(product2Result.getPrice()).hasValue(product2.price());
    assertThat(product2Result.getCreated()).hasValue(product2.created());
    assertThat(product2Result.getDescription()).hasValue(product2.description());

    assertThat(
            session
                .execute(
                    SimpleStatement.newInstance(
                        "SELECT * FROM \"Orders\" WHERE \"prodName\" = ?", productName))
                .one())
        .isNotNull()
        .extracting(r -> r.getString("\"customerName\""), r -> r.getString("description"))
        .containsExactly(customerName, description);
  }

  private InsertOrdersAndBulkInsertProductsWithAtomicMutation.Data
      insertOrdersAndBulkInsertProductsWthAtomic(
          ApolloClient client, InsertOrdersAndBulkInsertProductsWithAtomicMutation mutation) {
    InsertOrdersAndBulkInsertProductsWithAtomicMutation.Data result =
        getObservable(client.mutate(mutation));
    assertThat(result.getProducts()).isPresent();
    assertThat(result.getOrder()).isPresent();
    return result;
  }

  private BulkInsertProductsAndOrdersWithAtomicMutation.Data bulkInsertProductsAndOrdersWithAtomic(
      ApolloClient client, BulkInsertProductsAndOrdersWithAtomicMutation mutation) {
    BulkInsertProductsAndOrdersWithAtomicMutation.Data result =
        getObservable(client.mutate(mutation));
    assertThat(result.getProducts()).isPresent();
    assertThat(result.getOrder()).isPresent();
    return result;
  }

  private List<BulkInsertProductsMutation.BulkInsertProduct> bulkInsertProducts(
      ApolloClient client, List<ProductsInput> productsInputs) {
    BulkInsertProductsMutation mutation =
        BulkInsertProductsMutation.builder().values(productsInputs).build();
    BulkInsertProductsMutation.Data result = getObservable(client.mutate(mutation));
    assertThat(result.getBulkInsertProducts()).isPresent();
    assertThat(result.getBulkInsertProducts()).isPresent();
    assertThat(result.getBulkInsertProducts().get().size()).isEqualTo(productsInputs.size());
    return result.getBulkInsertProducts().get();
  }

  private List<BulkInsertProductsWithAtomicMutation.BulkInsertProduct> bulkInsertProductsWithAtomic(
      ApolloClient client, List<ProductsInput> productsInputs) {
    BulkInsertProductsWithAtomicMutation mutation =
        BulkInsertProductsWithAtomicMutation.builder().values(productsInputs).build();
    BulkInsertProductsWithAtomicMutation.Data result = getObservable(client.mutate(mutation));
    assertThat(result.getBulkInsertProducts()).isPresent();
    assertThat(result.getBulkInsertProducts()).isPresent();
    assertThat(result.getBulkInsertProducts().get().size()).isEqualTo(productsInputs.size());
    return result.getBulkInsertProducts().get();
  }
}
