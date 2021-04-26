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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class LtPredicateTest {

    LtPredicate lt = LtPredicate.of();

    @Nested
    class PredicateTest {

        @Test
        public void stringEquals() {
            boolean result = lt.test("filterValue", "filterValue");

            assertThat(result).isFalse();
        }

        @Test
        public void stringGreater() {
            boolean result = lt.test("filterValue", "aaa");

            assertThat(result).isFalse();
        }

        @Test
        public void stringLess() {
            boolean result = lt.test("filterValue", "www");

            assertThat(result).isTrue();
        }

        @Test
        public void stringNull() {
            boolean result = lt.test("filterValue", null);

            // nulls last
            assertThat(result).isTrue();
        }

        @Test
        public void booleanEquals() {
            boolean value = RandomUtils.nextBoolean();

            boolean result = lt.test(value, value);

            assertThat(result).isFalse();
        }

        @Test
        public void booleanGreater() {
            boolean result = lt.test(true, false);

            assertThat(result).isFalse();
        }

        @Test
        public void booleanLess() {
            boolean result = lt.test(false, true);

            assertThat(result).isTrue();
        }

        @Test
        public void booleanNull() {
            boolean result = lt.test(true, null);

            assertThat(result).isTrue();
        }

        @Test
        public void numberEquals() {
            boolean result = lt.test(22d, 22d);

            assertThat(result).isFalse();
        }

        @Test
        public void numbersGreater() {
            boolean result = lt.test(22.1d, 22d);

            assertThat(result).isFalse();
        }

        @Test
        public void numbersLess() {
            boolean result = lt.test(21.9d, 22d);

            assertThat(result).isTrue();
        }

        @Test
        public void numbersNull() {
            boolean result = lt.test(22, null);

            assertThat(result).isTrue();
        }

    }

    @Nested
    class GetDatabasePredicate {

        @Test
        public void correct() {
            Optional<Predicate> result = lt.getDatabasePredicate();

            assertThat(result).hasValue(Predicate.LT);
        }

    }

    @Nested
    class GetRawValue {

        @Test
        public void correct() {
            String result = lt.getRawValue();

            assertThat(result).isEqualTo("$lt");
        }

    }

}
