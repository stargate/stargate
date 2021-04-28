package io.stargate.web.docsapi.service.query.condition.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExistsConditionTest {

  @Nested
  class Constructor {

    @Test
    public void validated() {
      Throwable throwable = catchThrowable(() -> ImmutableExistsCondition.of(false));

      assertThat(throwable).isInstanceOf(DocumentAPIRequestException.class);
    }

    @Test
    public void assertProps() {
      ExistsCondition condition = ImmutableExistsCondition.of(true);

      assertThat(condition.isPersistenceCondition()).isTrue();
      assertThat(condition.getBuiltCondition()).isEmpty();
      assertThat(condition.test(null)).isTrue();
    }
  }
}
