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

package io.stargate.web.docsapi.service.query.filter.operation.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.db.query.Predicate;
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class EqFilterOperationTest {

  EqFilterOperation eq = EqFilterOperation.of();

  @Nested
  class FilterTest {

    @Test
    public void stringEquals() {
      boolean result = eq.test("filterValue", "filterValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringNotEquals() {
      boolean result = eq.test("dbValue", "filterValue");

      assertThat(result).isFalse();
    }

    @Test
    public void stringNotEqualsNull() {
      boolean result = eq.test(null, "filterValue");

      assertThat(result).isFalse();
    }

    @Test
    public void booleanEquals() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = eq.test(value, value);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanNotEquals() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = eq.test(!value, value);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNotEqualsNull() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = eq.test(null, value);

      assertThat(result).isFalse();
    }

    @Test
    public void numberEquals() {
      boolean result = eq.test(22d, 22d);

      assertThat(result).isTrue();
    }

    @Test
    public void numberEqualsDifferentTypes() {
      boolean result = eq.test(22d, 22);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersNotEquals() {
      boolean result = eq.test(23d, 22d);

      assertThat(result).isFalse();
    }

    @Test
    public void numbersNotEqualsDifferentTypes() {
      boolean result = eq.test(22.01, 22L);

      assertThat(result).isFalse();
    }

    @Test
    public void numbersNotEqualsNull() {
      boolean result = eq.test(null, 22);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class GetDatabasePredicate {

    @Test
    public void correct() {
      Optional<Predicate> result = eq.getQueryPredicate();

      assertThat(result).hasValue(Predicate.EQ);
    }
  }

  @Nested
  class GetOpCode {

    @Test
    public void correct() {
      FilterOperationCode result = eq.getOpCode();

      assertThat(result).isEqualTo(FilterOperationCode.EQ);
    }
  }

  @Nested
  class Negation {
    @Test
    public void negate() {
      assertThat(eq.negate()).isInstanceOf(NeFilterOperation.class);
    }
  }
}
