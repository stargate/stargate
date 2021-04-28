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
import java.util.Optional;

import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class GteFilterOperationTest {

  GteFilterOperation gte = GteFilterOperation.of();

  @Nested
  class FilterTest {

    @Test
    public void stringEquals() {
      boolean result = gte.test("filterValue", "filterValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringGreater() {
      boolean result = gte.test("filterValue", "aaa");

      assertThat(result).isTrue();
    }

    @Test
    public void stringLess() {
      boolean result = gte.test("filterValue", "www");

      assertThat(result).isFalse();
    }

    @Test
    public void stringNull() {
      boolean result = gte.test("filterValue", null);

      // nulls last
      assertThat(result).isFalse();
    }

    @Test
    public void booleanEquals() {
      boolean value = RandomUtils.nextBoolean();

      boolean result = gte.test(value, value);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanGreater() {
      boolean result = gte.test(true, false);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanLess() {
      boolean result = gte.test(false, true);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNull() {
      boolean result = gte.test(true, null);

      assertThat(result).isFalse();
    }

    @Test
    public void numberEquals() {
      boolean result = gte.test(22d, 22d);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersGreater() {
      boolean result = gte.test(22.1d, 22d);

      assertThat(result).isTrue();
    }

    @Test
    public void numbersLess() {
      boolean result = gte.test(21.9d, 22d);

      assertThat(result).isFalse();
    }

    @Test
    public void numbersNull() {
      boolean result = gte.test(22, null);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class GetDatabasePredicate {

    @Test
    public void correct() {
      Optional<Predicate> result = gte.getQueryPredicate();

      assertThat(result).hasValue(Predicate.GTE);
    }
  }

  @Nested
  class GetOpCode {

    @Test
    public void correct() {
      FilterOperationCode result = gte.getOpCode();

      assertThat(result).isEqualTo(FilterOperationCode.GTE);
    }
  }
}
