package io.stargate.it.http.graphql.cqlfirst;

import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import com.apollographql.apollo.ApolloClient;
import com.apollographql.apollo.api.Error;
import com.example.graphql.client.betterbotz.aggregations.*;
import com.example.graphql.client.betterbotz.orders.InsertOrdersMutation;
import com.example.graphql.client.betterbotz.type.OrdersFilterInput;
import com.example.graphql.client.betterbotz.type.OrdersInput;
import com.example.graphql.client.betterbotz.type.StringFilterInput;
import java.math.BigDecimal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class AggregationFunctionsTest extends ApolloTestBase {
  @Test
  @DisplayName("Should calculate number of orders using count aggregation")
  public void insertOrdersAndCountUsingBigIntFunctionWithAlias() {
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price("3000")
            .description("d1")
            .build();
    OrdersInput order2 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c2")
            .price("2500")
            .description("d2")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    InsertOrdersMutation order2Mutation = InsertOrdersMutation.builder().value(order2).build();

    assertThat(getObservable(client.mutate(order1Mutation)).getInsertOrders().isPresent()).isTrue();
    assertThat(getObservable(client.mutate(order2Mutation)).getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();

    GetOrdersWithCountQuery query = GetOrdersWithCountQuery.builder().filter(filterInput).build();

    GetOrdersWithCountQuery.Data result = getObservable(client.query(query));

    assertThat(result.getOrders().get().getValues().get().get(0).getCount().get()).isEqualTo("2");
  }

  @Test
  @DisplayName("Should calculate average of orders' price using avg aggregation")
  public void insertOrdersAndAvgUsingDecimalFunctionWithoutAlias() {
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price("4")
            .description("d1")
            .build();
    OrdersInput order2 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c2")
            .price("10")
            .description("d2")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    InsertOrdersMutation order2Mutation = InsertOrdersMutation.builder().value(order2).build();

    assertThat(getObservable(client.mutate(order1Mutation)).getInsertOrders().isPresent()).isTrue();
    assertThat(getObservable(client.mutate(order2Mutation)).getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();

    GetOrdersWithAvgQuery query = GetOrdersWithAvgQuery.builder().filter(filterInput).build();

    GetOrdersWithAvgQuery.Data result = getObservable(client.query(query));

    // avg price is (4 + 10) / 2 = 7
    assertThat(result.getOrders().get().getValues().get().get(0).get_decimal_function().get())
        .isEqualTo("7");
  }

  @Test
  @DisplayName("Should get the highest order using max aggregation")
  public void insertOrdersAndMaxUsingDecimalFunctionWithAlias() {
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price("3000")
            .description("d1")
            .build();
    OrdersInput order2 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c2")
            .price("2500")
            .description("d2")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    InsertOrdersMutation order2Mutation = InsertOrdersMutation.builder().value(order2).build();

    assertThat(getObservable(client.mutate(order1Mutation)).getInsertOrders().isPresent()).isTrue();
    assertThat(getObservable(client.mutate(order2Mutation)).getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();

    GetOrdersWithMaxQuery query = GetOrdersWithMaxQuery.builder().filter(filterInput).build();

    GetOrdersWithMaxQuery.Data result = getObservable(client.query(query));

    assertThat(result.getOrders().get().getValues().get().get(0).getMax().get()).isEqualTo("3000");
  }

  @Test
  @DisplayName("Should get the lowest order using min aggregation")
  public void insertOrdersAndMinUsingDecimalFunctionWithAlias() {
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price("3000")
            .description("d1")
            .build();
    OrdersInput order2 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c2")
            .price("2500")
            .description("d2")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    InsertOrdersMutation order2Mutation = InsertOrdersMutation.builder().value(order2).build();

    assertThat(getObservable(client.mutate(order1Mutation)).getInsertOrders().isPresent()).isTrue();
    assertThat(getObservable(client.mutate(order2Mutation)).getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();
    GetOrdersWithMinQuery query = GetOrdersWithMinQuery.builder().filter(filterInput).build();

    GetOrdersWithMinQuery.Data result = getObservable(client.query(query));

    assertThat(result.getOrders().get().getValues().get().get(0).getMin().get()).isEqualTo("2500");
  }

  @Test
  @DisplayName("Should sum all orders' price using sum aggregation")
  public void insertOrdersAndSumUsingDecimalFunctionWithAlias() {
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price("3000")
            .description("d1")
            .build();
    OrdersInput order2 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c2")
            .price("2500")
            .description("d2")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    InsertOrdersMutation order2Mutation = InsertOrdersMutation.builder().value(order2).build();

    assertThat(getObservable(client.mutate(order1Mutation)).getInsertOrders().isPresent()).isTrue();
    assertThat(getObservable(client.mutate(order2Mutation)).getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();
    GetOrdersWithSumQuery query = GetOrdersWithSumQuery.builder().filter(filterInput).build();

    GetOrdersWithSumQuery.Data result = getObservable(client.query(query));

    assertThat(result.getOrders().get().getValues().get().get(0).getSum().get()).isEqualTo("5500");
  }

  @Test
  @DisplayName("Should test all available function types")
  public void testAllGraphqlFunctionTypes() {

    Number value = 100;
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price(value.toString())
            .value_int(value.intValue())
            .value_double(value.doubleValue())
            .value_bigint(value.toString())
            .value_varint(value.toString())
            .value_float(value.doubleValue())
            .value_smallint(value.shortValue())
            .value_tinyint(value.byteValue())
            .description("d1")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    InsertOrdersMutation.Data result = getObservable(client.mutate(order1Mutation));
    assertThat(result.getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();

    GetOrdersAllFunctionsQuery query =
        GetOrdersAllFunctionsQuery.builder().filter(filterInput).build();

    GetOrdersAllFunctionsQuery.Data functionsResult = getObservable(client.query(query));

    GetOrdersAllFunctionsQuery.Value res =
        functionsResult.getOrders().get().getValues().get().get(0);
    assertThat(res.get_int_function().get()).isEqualTo(value.intValue());
    assertThat(res.get_double_function().get()).isEqualTo(value.doubleValue());
    assertThat(res.get_bigint_function().get()).isEqualTo(value.toString());
    assertThat(res.get_decimal_function().get()).isEqualTo(value.toString());
    assertThat(res.get_varint_function().get()).isEqualTo(value.toString());
    assertThat(res.get_float_function().get()).isEqualTo(value.doubleValue());
    assertThat(res.get_smallint_function().get()).isEqualTo(BigDecimal.valueOf(value.shortValue()));
    assertThat(res.get_tinyint_function().get()).isEqualTo(BigDecimal.valueOf(value.byteValue()));
  }

  @Test
  @DisplayName("Should error when trying to use an unknown aggregation function")
  public void shouldErrorWhenTryingToUseAnUnknownAggregationFunction() {
    OrdersInput order1 =
        OrdersInput.builder()
            .prodName("p1")
            .customerName("c1")
            .price("3000")
            .description("d1")
            .build();

    ApolloClient client = getApolloClient("/graphql/" + keyspace);
    InsertOrdersMutation order1Mutation = InsertOrdersMutation.builder().value(order1).build();

    assertThat(getObservable(client.mutate(order1Mutation)).getInsertOrders().isPresent()).isTrue();

    OrdersFilterInput filterInput =
        OrdersFilterInput.builder().prodName(StringFilterInput.builder().eq("p1").build()).build();

    GetOrdersWithUnknownFunctionQuery query =
        GetOrdersWithUnknownFunctionQuery.builder().filter(filterInput).build();

    GraphQLTestException ex =
        catchThrowableOfType(() -> getObservable(client.query(query)), GraphQLTestException.class);

    assertThat(ex).isNotNull();
    assertThat(ex.errors)
        // One error per query
        .hasSize(1)
        .first()
        .extracting(Error::getMessage)
        .asString()
        .contains("The aggregation function: some_unknown_function is not supported");
  }
}
