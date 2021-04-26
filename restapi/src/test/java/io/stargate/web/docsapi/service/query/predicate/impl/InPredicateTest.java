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

package io.stargate.web.docsapi.service.query.predicate.impl;

import io.stargate.db.query.Predicate;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class InPredicateTest {

    InPredicate in = InPredicate.of();

    @Nested
    class PredicateTest {

        @Test
        public void stringIn() {
            List<?> filterValue = Collections.singletonList("filterValue");

            boolean result = in.test(filterValue, "filterValue");

            assertThat(result).isTrue();
        }

        @Test
        public void stringNotIn() {
            List<?> filterValue = Collections.singletonList("filterValue");

            boolean result = in.test(filterValue, "dbValue");

            assertThat(result).isFalse();
        }

        @Test
        public void stringNullIn() {
            List<?> filterValue = new ArrayList<>();
            filterValue.add(null);

            boolean result = in.test(filterValue, (String) null);

            assertThat(result).isTrue();
        }

        @Test
        public void stringNullNotIn() {
            List<?> filterValue = Collections.singletonList("filterValue");

            boolean result = in.test(filterValue, (String) null);

            assertThat(result).isFalse();
        }

        @Test
        public void booleanIn() {
            boolean value = RandomUtils.nextBoolean();
            List<?> filterValue = Collections.singletonList(value);

            boolean result = in.test(filterValue, value);

            assertThat(result).isTrue();
        }

        @Test
        public void booleanNotIn() {
            boolean value = RandomUtils.nextBoolean();
            List<?> filterValue = Collections.singletonList(value);

            boolean result = in.test(filterValue, !value);

            assertThat(result).isFalse();
        }

        @Test
        public void booleanNullIn() {
            List<?> filterValue = new ArrayList<>();
            filterValue.add(null);

            boolean result = in.test(filterValue, (Boolean) null);

            assertThat(result).isTrue();
        }

        @Test
        public void booleanNullNotIn() {
            boolean value = RandomUtils.nextBoolean();
            List<?> filterValue = Collections.singletonList(value);

            boolean result = in.test(filterValue, (Boolean) null);

            assertThat(result).isFalse();
        }

        @Test
        public void numberIn() {
            List<?> filterValue = Collections.singletonList(22d);

            boolean result = in.test(filterValue, 22d);

            assertThat(result).isTrue();
        }

        @Test
        public void numberNotIn() {
            List<?> filterValue = Collections.singletonList(22d);

            boolean result = in.test(filterValue, 23d);

            assertThat(result).isFalse();
        }

        @Test
        public void numberNullIn() {
            List<?> filterValue = new ArrayList<>();
            filterValue.add(null);

            boolean result = in.test(filterValue, (Double) null);

            assertThat(result).isTrue();
        }

        @Test
        public void numberNullNotIn() {
            List<?> filterValue = Collections.singletonList(22d);

            boolean result = in.test(filterValue, (Double) null);

            assertThat(result).isFalse();
        }

    }

    @Nested
    class ValidateBooleanFilterInput {

        @Test
        public void isNull() {
            Throwable throwable = catchThrowable(() -> in.validateBooleanFilterInput(null));

            assertThat(throwable).isNotNull();
        }

        @Test
        public void isEmpty() {
            Throwable throwable = catchThrowable(() -> in.validateBooleanFilterInput(Collections.emptyList()));

            assertThat(throwable).isNotNull();
        }

        @Test
        public void isNotEmpty() {
            Throwable throwable = catchThrowable(() -> in.validateBooleanFilterInput(Collections.singletonList(true)));

            assertThat(throwable).isNull();
        }

    }

    @Nested
    class ValidateStringFilterInput {

        @Test
        public void isNull() {
            Throwable throwable = catchThrowable(() -> in.validateStringFilterInput(null));

            assertThat(throwable).isNotNull();
        }

        @Test
        public void isEmpty() {
            Throwable throwable = catchThrowable(() -> in.validateStringFilterInput(Collections.emptyList()));

            assertThat(throwable).isNotNull();
        }

        @Test
        public void isNotEmpty() {
            Throwable throwable = catchThrowable(() -> in.validateStringFilterInput(Collections.singletonList("Jordan")));

            assertThat(throwable).isNull();
        }

    }

    @Nested
    class ValidateDoubleFilterInput {

        @Test
        public void isNull() {
            Throwable throwable = catchThrowable(() -> in.validateDoubleFilterInput(null));

            assertThat(throwable).isNotNull();
        }


        @Test
        public void isEmpty() {
            Throwable throwable = catchThrowable(() -> in.validateDoubleFilterInput(Collections.emptyList()));

            assertThat(throwable).isNotNull();
        }

        @Test
        public void isNotEmpty() {
            Throwable throwable = catchThrowable(() -> in.validateDoubleFilterInput(Collections.singletonList(23)));

            assertThat(throwable).isNull();
        }

    }

    @Nested
    class IsMatchAll {

        @Test
        public void correct() {
            boolean result = in.isMatchAll();

            assertThat(result).isFalse();
        }

    }

    @Nested
    class GetDatabasePredicate {

        @Test
        public void correct() {
            Optional<Predicate> result = in.getDatabasePredicate();

            assertThat(result).isEmpty();
        }

    }

    @Nested
    class GetRawValue {

        @Test
        public void correct() {
            String result = in.getRawValue();

            assertThat(result).isEqualTo("$in");
        }

    }

}
