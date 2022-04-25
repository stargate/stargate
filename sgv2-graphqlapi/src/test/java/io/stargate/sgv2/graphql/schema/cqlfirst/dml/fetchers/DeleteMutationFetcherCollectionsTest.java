package io.stargate.sgv2.graphql.schema.cqlfirst.dml.fetchers;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass.Value;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.graphql.schema.SampleKeyspaces;
import io.stargate.sgv2.graphql.schema.cqlfirst.dml.DmlTestBase;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DeleteMutationFetcherCollectionsTest extends DmlTestBase {

  @Override
  protected List<Schema.CqlKeyspaceDescribe> getCqlSchema() {
    return ImmutableList.of(SampleKeyspaces.COLLECTIONS);
  }

  @ParameterizedTest
  @MethodSource("successfulQueries")
  @DisplayName("Should execute GraphQL with collections and generate expected CQL query")
  public void collectionsTest(
      String graphQlQuery, String expectedCqlQuery, List<Value> expectedValues) {
    assertQuery(String.format("mutation { %s }", graphQlQuery), expectedCqlQuery, expectedValues);
  }

  public static Arguments[] successfulQueries() {
    return new Arguments[] {
      arguments(
          "deleteRegularListTable(value: {k: 1}, ifCondition: { l: {notEq: [1,2,3] } }) { applied }",
          "DELETE FROM collections.\"RegularListTable\" WHERE k = ? IF l != ?",
          ImmutableList.of(Values.of(1), listV(1, 2, 3))),
      arguments(
          "deleteRegularSetTable(value: {k: 1}, ifCondition: { s: {notEq: [1,2,3] } }) { applied }",
          "DELETE FROM collections.\"RegularSetTable\" WHERE k = ? IF s != ?",
          ImmutableList.of(Values.of(1), listV(1, 2, 3))),
      arguments(
          "deleteRegularMapTable(value: {k: 1},"
              + "  ifCondition: { m: {notEq: [{key: 1,value:\"a\"},{key: 2,value:\"b\"}] } }) "
              + "{ applied }",
          "DELETE FROM collections.\"RegularMapTable\" WHERE k = ? IF m != ?",
          ImmutableList.of(Values.of(1), listV(1, "a", 2, "b"))),
    };
  }
}
