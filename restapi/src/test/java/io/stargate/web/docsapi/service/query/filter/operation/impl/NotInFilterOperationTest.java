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

      boolean result = nin.test(filterValue, "filterValue");

      assertThat(result).isFalse();
    }

    @Test
    public void stringNotIn() {
      List<?> filterValue = Collections.singletonList("filterValue");

      boolean result = nin.test(filterValue, "dbValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringNullIn() {
      List<?> filterValue = new ArrayList<>();
      filterValue.add(null);

      boolean result = nin.test(filterValue, (String) null);

      assertThat(result).isFalse();
    }

    @Test
    public void stringNullNotIn() {
      List<?> filterValue = Collections.singletonList("filterValue");

      boolean result = nin.test(filterValue, (String) null);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanIn() {
      boolean value = RandomUtils.nextBoolean();
      List<?> filterValue = Collections.singletonList(value);

      boolean result = nin.test(filterValue, value);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNotIn() {
      boolean value = RandomUtils.nextBoolean();
      List<?> filterValue = Collections.singletonList(value);

      boolean result = nin.test(filterValue, !value);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanNullIn() {
      List<?> filterValue = new ArrayList<>();
      filterValue.add(null);

      boolean result = nin.test(filterValue, (Boolean) null);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanNullNotIn() {
      boolean value = RandomUtils.nextBoolean();
      List<?> filterValue = Collections.singletonList(value);

      boolean result = nin.test(filterValue, (Boolean) null);

      assertThat(result).isTrue();
    }

    @Test
    public void numberIn() {
      List<?> filterValue = Collections.singletonList(22d);

      boolean result = nin.test(filterValue, 22d);

      assertThat(result).isFalse();
    }

    @Test
    public void numberNotIn() {
      List<?> filterValue = Collections.singletonList(22d);

      boolean result = nin.test(filterValue, 23d);

      assertThat(result).isTrue();
    }

    @Test
    public void numberNullIn() {
      List<?> filterValue = new ArrayList<>();
      filterValue.add(null);

      boolean result = nin.test(filterValue, (Double) null);

      assertThat(result).isFalse();
    }

    @Test
    public void numberNullNotIn() {
      List<?> filterValue = Collections.singletonList(22d);

      boolean result = nin.test(filterValue, (Double) null);

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
}
