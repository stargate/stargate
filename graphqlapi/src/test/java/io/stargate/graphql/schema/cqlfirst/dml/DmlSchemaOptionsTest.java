package io.stargate.graphql.schema.cqlfirst.dml;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.schema.GraphQLEnumValueDefinition;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.util.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DmlSchemaOptionsTest {

  @ParameterizedTest
  @MethodSource
  public void testConsistencyLevelParsing(String csvLevels, List<String> expectedValues) {
    List<GraphQLEnumValueDefinition> values =
        DmlSchemaBuilder.getConsistencyEnum(csvLevels, "Name").getValues();

    assertThat(values)
        .extracting(GraphQLEnumValueDefinition::getName)
        .hasSameElementsAs(expectedValues);
  }

  static Stream<Arguments> testConsistencyLevelParsing() {
    return Stream.of(
        Arguments.of("LOCAL_QUORUM", Lists.newArrayList("LOCAL_QUORUM")),
        Arguments.of("LOCAL_QUORUM,EACH_QUORUM", Lists.newArrayList("LOCAL_QUORUM", "EACH_QUORUM")),
        Arguments.of(
            "LOCAL_QUORUM,EACH_QUORUM,ALL",
            Lists.newArrayList("LOCAL_QUORUM", "EACH_QUORUM", "ALL")),
        Arguments.of(
            "LOCAL_QUORUM,QUORUM,EACH_QUORUM,ALL",
            Lists.newArrayList("LOCAL_QUORUM", "QUORUM", "EACH_QUORUM", "ALL")),
        // Local quorum will always be added as it's the default
        Arguments.of("EACH_QUORUM", Lists.newArrayList("LOCAL_QUORUM", "EACH_QUORUM")),
        Arguments.of(
            "monkey,banana,fish,QUORUM,brains", Lists.newArrayList("LOCAL_QUORUM", "QUORUM")),
        // Invalid quorum levels should be skipped
        Arguments.of("", Lists.newArrayList("LOCAL_QUORUM")),
        // Case insensitivity
        Arguments.of("each_quorum", Lists.newArrayList("LOCAL_QUORUM", "EACH_QUORUM")));
  }
}
