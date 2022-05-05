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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class NotInFilterOperationTest {

  NotInFilterOperation nin = NotInFilterOperation.of();

  @Nested
  class FilterTest {

    @Test
    public void stringIn() {
      List<?> filterValue = Collections.singletonList("filterValue");

      boolean result = nin.test("filterValue", filterValue);

      assertThat(result).isFalse();
    }

    @Test
    public void stringNotIn() {
      List<?> filterValue = Collections.singletonList("filterValue");

      boolean result = nin.test("dbValue", filterValue);

      assertThat(result).isTrue();
    }

    @Test
    public void stringNullIn() {
      List<?> filterValue = new ArrayList<>();
      filterValue.add(null);

      boolean result = nin.test((String) null, filterValue);

      assertThat(result).isFalse();
    }

    @Test
    public void stringNullNotIn() {
      List<?> filterValue = Collections.singletonList("filterValue");

      boolean result = nin.test((String) null, filterValue);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanIn() {
      boolean value = RandomUtils.nextBoolean();
      List<?> filterValue = Collections.singletonList(value);

      boolean result = nin.test(value, filterValue);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNotIn() {
      boolean value = RandomUtils.nextBoolean();
      List<?> filterValue = Collections.singletonList(value);

      boolean result = nin.test(!value, filterValue);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanNullIn() {
      List<?> filterValue = new ArrayList<>();
      filterValue.add(null);

      boolean result = nin.test((Boolean) null, filterValue);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNullNotIn() {
      boolean value = RandomUtils.nextBoolean();
      List<?> filterValue = Collections.singletonList(value);

      boolean result = nin.test((Boolean) null, filterValue);

      assertThat(result).isTrue();
    }

    @Test
    public void numberIn() {
      List<?> filterValue = Collections.singletonList(22d);

      boolean result = nin.test(22d, filterValue);

      assertThat(result).isFalse();
    }

    @Test
    public void numberNotIn() {
      List<?> filterValue = Collections.singletonList(22d);

      boolean result = nin.test(23d, filterValue);

      assertThat(result).isTrue();
    }

    @Test
    public void numberNullIn() {
      List<?> filterValue = new ArrayList<>();
      filterValue.add(null);

      boolean result = nin.test((Double) null, filterValue);

      assertThat(result).isFalse();
    }

    @Test
    public void numberNullNotIn() {
      List<?> filterValue = Collections.singletonList(22d);

      boolean result = nin.test((Double) null, filterValue);

      assertThat(result).isTrue();
    }
  }

  @Nested
  class GetDatabasePredicate {

    @Test
    public void correct() {
      Optional<Predicate> result = nin.getQueryPredicate();

      assertThat(result).isEmpty();
    }
  }

  @Nested
  class GetOpCode {

    @Test
    public void correct() {
      FilterOperationCode result = nin.getOpCode();

      assertThat(result).isEqualTo(FilterOperationCode.NIN);
    }
  }

  @Nested
  class IsEvaluateOnMissingFields {

    @Test
    public void evaluate() {
      boolean result = nin.isEvaluateOnMissingFields();

      assertThat(result).isTrue();
    }
  }

  @Nested
  class Negation {
    @Test
    public void negate() {
      assertThat(nin.negate()).isEqualTo(InFilterOperation.of());
    }
  }
}
