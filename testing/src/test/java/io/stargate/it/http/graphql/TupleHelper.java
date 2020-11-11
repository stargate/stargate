package io.stargate.it.http.graphql;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.graphql.client.betterbotz.tuples.GetTuplesQuery;
import com.example.graphql.client.betterbotz.type.TupleBigIntInput;
import com.example.graphql.client.betterbotz.type.TupleFloat32Float32Input;
import com.example.graphql.client.betterbotz.type.TupleTimeUuidIntBooleanInput;
import com.example.graphql.client.betterbotz.type.TuplesInput;
import java.util.UUID;
import org.assertj.core.api.InstanceOfAssertFactories;

/** Contains helper methods to test tuples in GraphQL */
public class TupleHelper {
  public static TuplesInput createTupleInput(
      UUID id, long tuple1, float[] tuple2, Object[] tuple3) {
    return TuplesInput.builder()
        .id(id)
        .tuple1(TupleBigIntInput.builder().item0(String.valueOf(tuple1)).build())
        .tuple2(TupleFloat32Float32Input.builder().item0(tuple2[0]).item1(tuple2[1]).build())
        .tuple3(
            TupleTimeUuidIntBooleanInput.builder()
                .item0(tuple3[0])
                .item1((int) tuple3[1])
                .item2((boolean) tuple3[2])
                .build())
        .build();
  }

  public static void assertTuples(
      GetTuplesQuery.Data result, long tuple1Value, float[] tuple2, Object[] tuple3) {
    assertThat(result.getTuples())
        .isPresent()
        .get()
        .extracting(v -> v.getValues(), InstanceOfAssertFactories.OPTIONAL)
        .isPresent()
        .get(InstanceOfAssertFactories.LIST)
        .hasSize(1);

    GetTuplesQuery.Value item = result.getTuples().get().getValues().get().get(0);
    assertThat(item.getTuple1())
        .isPresent()
        .get()
        .extracting(v -> v.getItem0().get())
        .isEqualTo(String.valueOf(tuple1Value));
    assertThat(item.getTuple2())
        .isPresent()
        .get()
        .extracting(v -> v.getItem0().get(), v -> v.getItem1().get())
        .containsExactly(tuple2[0], tuple2[1]);
    assertThat(item.getTuple3())
        .isPresent()
        .get()
        .extracting(v -> v.getItem0().get(), v -> v.getItem1().get(), v -> v.getItem2().get())
        .containsExactly(tuple3[0].toString(), tuple3[1], tuple3[2]);
  }
}
