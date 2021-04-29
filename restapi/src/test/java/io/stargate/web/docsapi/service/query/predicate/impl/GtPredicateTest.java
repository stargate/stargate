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

class GtPredicateTest {

    GtPredicate gt = GtPredicate.of();

    @Nested
    class PredicateTest {

        @Test
        public void stringEquals() {
            boolean result = gt.test("filterValue", "filterValue");

            assertThat(result).isFalse();
        }

        @Test
        public void stringGreater() {
            boolean result = gt.test("filterValue", "aaa");

            assertThat(result).isTrue();
        }

        @Test
        public void stringLess() {
            boolean result = gt.test("filterValue", "www");

            assertThat(result).isFalse();
        }

        @Test
        public void stringNull() {
            boolean result = gt.test("filterValue", null);

            // nulls last
            assertThat(result).isFalse();
        }

        @Test
        public void booleanEquals() {
            boolean value = RandomUtils.nextBoolean();

            boolean result = gt.test(value, value);

            assertThat(result).isFalse();
        }

        @Test
        public void booleanGreater() {
            boolean result = gt.test(true, false);

            assertThat(result).isTrue();
        }

        @Test
        public void booleanLess() {
            boolean result = gt.test(false, true);

            assertThat(result).isFalse();
        }

        @Test
        public void booleanNull() {
            boolean result = gt.test(true, null);

            assertThat(result).isFalse();
        }

        @Test
        public void numberEquals() {
            boolean result = gt.test(22d, 22d);

            assertThat(result).isFalse();
        }

        @Test
        public void numbersGreater() {
            boolean result = gt.test(22.1d, 22d);

            assertThat(result).isTrue();
        }

        @Test
        public void numbersLess() {
            boolean result = gt.test(21.9d, 22d);

            assertThat(result).isFalse();
        }

        @Test
        public void numbersNull() {
            boolean result = gt.test(22, null);

            assertThat(result).isFalse();
        }

    }

    @Nested
    class GetDatabasePredicate {

        @Test
        public void correct() {
            Optional<Predicate> result = gt.getDatabasePredicate();

            assertThat(result).hasValue(Predicate.GT);
        }

    }

    @Nested
    class GetRawValue {

        @Test
        public void correct() {
            String result = gt.getRawValue();

            assertThat(result).isEqualTo("$gt");
        }

    }

}
