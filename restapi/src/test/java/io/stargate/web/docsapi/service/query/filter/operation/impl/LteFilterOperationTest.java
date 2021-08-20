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

class LteFilterOperationTest {

  LteFilterOperation lte = LteFilterOperation.of();

  @Nested
  class FilterTest {

    @Test
    public void stringEquals() {
      boolean result = lte.test("filterValue", "filterValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringLess() {
      boolean result = lte.test("aaa", "filterValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringGreater() {
      boolean result = lte.test("www", "filterValue");

      assertThat(result).isFalse();
    }

    @Test
    public void stringNull() {
      boolean result = lte.test(null, "filterValue");

      // nulls last
      assertThat(result).isFalse();
    }

    @Test
    public void booleanEquals() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = lte.test(value, value);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanLess() {
      boolean result = lte.test(false, true);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanGreater() {
      boolean result = lte.test(true, false);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNull() {
      boolean result = lte.test(null, true);

      assertThat(result).isFalse();
    }

    @Test
    public void numberEquals() {
      boolean result = lte.test(22d, 22d);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersLess() {
      boolean result = lte.test(22d, 22.1d);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersGreater() {
      boolean result = lte.test(22d, 21.9d);

      assertThat(result).isFalse();
    }

    @Test
    public void numbersNull() {
      boolean result = lte.test(null, 22);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class GetDatabasePredicate {

    @Test
    public void correct() {
      Optional<Predicate> result = lte.getQueryPredicate();

      assertThat(result).hasValue(Predicate.LTE);
    }
  }

  @Nested
  class GetRawValue {

    @Test
    public void getOpCode() {
      FilterOperationCode result = lte.getOpCode();

      assertThat(result).isEqualTo(FilterOperationCode.LTE);
    }
  }

  @Nested
  class Negation {
    @Test
    public void negate() {
      assertThat(lte.negate()).isInstanceOf(GtFilterOperation.class);
    }
  }
}
