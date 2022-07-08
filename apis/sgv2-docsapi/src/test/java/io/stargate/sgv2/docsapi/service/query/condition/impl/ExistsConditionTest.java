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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.docsapi.service.query.condition.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.docsapi.api.common.properties.document.DocumentProperties;
import io.stargate.sgv2.docsapi.service.common.model.RowWrapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExistsConditionTest {

  @Mock DocumentProperties documentProperties;

  @Nested
  class Constructor {

    @Test
    public void assertPropsTrue() {
      ExistsCondition condition = ImmutableExistsCondition.of(true, documentProperties);

      assertThat(condition.isPersistenceCondition()).isTrue();
      assertThat(condition.isEvaluateOnMissingFields()).isFalse();
      assertThat(condition.getBuiltCondition()).isEmpty();
    }

    @Test
    public void assertPropsFalse() {
      ExistsCondition condition = ImmutableExistsCondition.of(false, documentProperties);

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
      ExistsCondition condition = ImmutableExistsCondition.of(true, documentProperties);

      assertThat(condition.test(row)).isTrue();
    }

    @Test
    public void assertPropsFalse() {
      ExistsCondition condition = ImmutableExistsCondition.of(false, documentProperties);

      assertThat(condition.test(row)).isFalse();
    }
  }

  @Nested
  class Negation {
    @ParameterizedTest
    @CsvSource({"true", "false"})
    void simple(boolean queryValue) {
      ExistsCondition condition = ImmutableExistsCondition.of(queryValue, documentProperties);

      assertThat(condition.negate())
          .isInstanceOfSatisfying(
              ExistsCondition.class,
              negated -> {
                assertThat(negated.getQueryValue()).isEqualTo(!queryValue);
                assertThat(negated.documentProperties()).isEqualTo(documentProperties);
              });
    }
  }
}
