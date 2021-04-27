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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExistsFilterOperationTest {

  ExistsFilterOperation exists = ExistsFilterOperation.of();

  @Nested
  class FilterTest {

    @Test
    public void stringExists() {
      boolean result = exists.test(true, "dbValue");

      assertThat(result).isTrue();
    }

    @Test
    public void stringNotExist() {
      boolean result = exists.test(true, (String) null);

      assertThat(result).isFalse();
    }

    @Test
    public void booleanExists() {
      boolean result = exists.test(true, false);

      assertThat(result).isTrue();
    }

    @Test
    public void booleanNotExist() {
      boolean result = exists.test(true, (Boolean) null);

      assertThat(result).isFalse();
    }

    @Test
    public void numberExists() {
      boolean result = exists.test(true, 22d);

      assertThat(result).isTrue();
    }

    @Test
    public void numberNotExist() {
      boolean result = exists.test(true, (Double) null);

      assertThat(result).isFalse();
    }
  }

  @Nested
  class IsMatchAll {

    @Test
    public void correct() {
      boolean result = exists.isMatchAll();

      assertThat(result).isFalse();
    }
  }

  @Nested
  class GetDatabasePredicate {

    @Test
    public void correct() {
      Optional<Predicate> result = exists.getDatabasePredicate();

      // failing on purpose for now, until resolved
      assertThat(result).isEmpty();
    }
  }

  @Nested
  class GetRawValue {

    @Test
    public void correct() {
      String result = exists.getRawValue();

      assertThat(result).isEqualTo("$exists");
    }
  }
}
