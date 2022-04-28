package io.stargate.sgv2.docsapi.service.query.condition.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.docsapi.model.RowWrapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExistsConditionTest {

  @Nested
  class Constructor {

    @Test
    public void assertPropsTrue() {
      ExistsCondition condition = ImmutableExistsCondition.of(true);

      assertThat(condition.isPersistenceCondition()).isTrue();
      assertThat(condition.isEvaluateOnMissingFields()).isFalse();
      assertThat(condition.getBuiltCondition()).isEmpty();
    }

    @Test
    public void assertPropsFalse() {
      ExistsCondition condition = ImmutableExistsCondition.of(false);

      assertThat(condition.isPersistenceCondition()).isFalse();
      assertThat(condition.isEvaluateOnMissingFields()).isTrue();
      assertThat(condition.getBuiltCondition()).isEmpty();
    }
  }

  @Nested
  class DoTest {

    @Mock RowWrapper row;

    @Test
    public void existsTrue() {
      ExistsCondition condition = ImmutableExistsCondition.of(true);

      assertThat(condition.test(row)).isTrue();
    }

    @Test
    public void assertPropsFalse() {
      ExistsCondition condition = ImmutableExistsCondition.of(false);

      assertThat(condition.test(row)).isFalse();
    }
  }

  @Nested
  class Negation {
    @ParameterizedTest
    @CsvSource({"true", "false"})
    void simple(boolean queryValue) {
      ExistsCondition condition = ImmutableExistsCondition.of(queryValue);

      assertThat(condition.negate())
          .isInstanceOfSatisfying(
              ExistsCondition.class,
              negated -> {
                assertThat(negated.getQueryValue()).isEqualTo(!queryValue);
              });
    }
  }
}
