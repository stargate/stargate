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
import static org.assertj.core.api.Assertions.catchThrowable;

import io.stargate.db.query.Predicate;
import io.stargate.web.docsapi.service.query.filter.operation.FilterOperationCode;
import io.stargate.web.docsapi.service.query.filter.operation.ValueFilterOperation;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class NotNullValueFilterOperationTest {

  NotNullValueFilterOperation predicate =
      new NotNullValueFilterOperation() {

        @Override
        public boolean isSatisfied(int compareValue) {
          return false;
        }

        @Override
        public FilterOperationCode getOpCode() {
          return null;
        }

        @Override
        public Optional<Predicate> getQueryPredicate() {
          return Optional.empty();
        }

        @Override
        public ValueFilterOperation negate() {
          throw new UnsupportedOperationException();
        }
      };

  @Nested
  class ValidateBooleanFilterInput {

    @Test
    public void isNull() {
      Throwable throwable = catchThrowable(() -> predicate.validateBooleanFilterInput(null));

      assertThat(throwable).isNotNull();
    }

    @Test
    public void isNotNull() {
      Throwable throwable = catchThrowable(() -> predicate.validateBooleanFilterInput(true));

      assertThat(throwable).isNull();
    }
  }

  @Nested
  class ValidateStringFilterInput {

    @Test
    public void isNull() {
      Throwable throwable = catchThrowable(() -> predicate.validateStringFilterInput(null));

      assertThat(throwable).isNotNull();
    }

    @Test
    public void isNotNull() {
      Throwable throwable = catchThrowable(() -> predicate.validateStringFilterInput("Jordan"));

      assertThat(throwable).isNull();
    }
  }

  @Nested
  class ValidateNumberFilterInput {

    @Test
    public void isNull() {
      Throwable throwable = catchThrowable(() -> predicate.validateNumberFilterInput(null));

      assertThat(throwable).isNotNull();
    }

    @Test
    public void isNotNull() {
      Throwable throwable = catchThrowable(() -> predicate.validateNumberFilterInput(23));

      assertThat(throwable).isNull();
    }
  }
}
