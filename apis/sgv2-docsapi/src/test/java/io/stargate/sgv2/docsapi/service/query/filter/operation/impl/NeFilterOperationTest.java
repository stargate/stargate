/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.sgv2.docsapi.service.query.filter.operation.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.common.cql.builder.Predicate;
import io.stargate.sgv2.docsapi.service.query.filter.operation.FilterOperationCode;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class NeFilterOperationTest {

  NeFilterOperation ne = NeFilterOperation.of();

  @Nested
  class FilterTest {

    @Test
    public void stringEquals() {
      boolean result = ne.test("filterValue", "filterValue");

      assertThat(result).isFalse();
    }

    @Test
    public void stringNotEquals() {
      boolean result = ne.test("dbValue", "filterValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringNotEqualsNull() {
      boolean result = ne.test(null, "filterValue");

      assertThat(result).isTrue();
    }

    @Test
    public void booleanEquals() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = ne.test(value, value);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNotEquals() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = ne.test(!value, value);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanNotEqualsNull() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = ne.test(null, value);

      assertThat(result).isTrue();
    }

    @Test
    public void numberEquals() {
      boolean result = ne.test(22d, 22d);

      assertThat(result).isFalse();
    }

    @Test
    public void numberEqualsDifferentTypes() {
      boolean result = ne.test(22d, 22);

      assertThat(result).isFalse();
    }

    @Test
    public void numbersNotEquals() {
      boolean result = ne.test(23d, 22d);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersNotEqualsDifferentTypes() {
      boolean result = ne.test(22.01, 22L);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersNotEqualsNull() {
      boolean result = ne.test(null, 22);

      assertThat(result).isTrue();
    }
  }

  @Nested
  class GetDatabasePredicate {

    @Test
    public void correct() {
      Optional<Predicate> result = ne.getQueryPredicate();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class GetOpCode {

    @Test
    public void correct() {
      FilterOperationCode result = ne.getOpCode();

      assertThat(result).isEqualTo(FilterOperationCode.NE);
    }
  }

  @Nested
  class IsEvaluateOnMissingFields {

    @Test
    public void evaluate() {
      boolean result = ne.isEvaluateOnMissingFields();

      assertThat(result).isTrue();
    }
  }

  @Nested
  class Negation {
    @Test
    public void negate() {
      assertThat(ne.negate()).isInstanceOf(EqFilterOperation.class);
    }
  }
}
