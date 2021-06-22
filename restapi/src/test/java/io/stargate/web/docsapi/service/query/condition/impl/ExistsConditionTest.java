package io.stargate.web.docsapi.service.query.condition.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExistsConditionTest {

  @Nested
  class Constructor {

    @Test
    public void validated() {
      Throwable throwable = catchThrowable(() -> ImmutableExistsCondition.of(false));

      assertThat(throwable)
          .isInstanceOf(ErrorCodeRuntimeException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.DOCS_API_SEARCH_FILTER_INVALID);
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
