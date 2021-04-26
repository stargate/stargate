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

package io.stargate.web.docsapi.service.query.condition.impl;

import io.stargate.db.datastore.Row;
import io.stargate.db.query.Predicate;
import io.stargate.db.query.builder.BuiltCondition;
import io.stargate.web.docsapi.service.query.predicate.BooleanValuePredicate;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BooleanConditionTest {

    @Mock
    BooleanValuePredicate<Boolean> predicate;

    @Nested
    class Constructor {

        @Test
        public void predicateValidated() {
            boolean value = true;

            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, value, true);

            verify(predicate).validateBooleanFilterInput(true);
        }

    }

    @Nested
    class GetBuiltCondition {

        @Test
        public void happyPath() {
            Predicate eq = Predicate.EQ;
            boolean value = RandomUtils.nextBoolean();
            when(predicate.getDatabasePredicate()).thenReturn(Optional.of(eq));

            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, value, true);
            Optional<BuiltCondition> result = condition.getBuiltCondition();

            assertThat(result).hasValueSatisfying(builtCondition -> {
                assertThat(builtCondition.predicate()).isEqualTo(eq);
                assertThat(builtCondition.value().get()).isEqualTo(value);
                assertThat(builtCondition.lhs()).isEqualTo(BuiltCondition.LHS.column("bool_value"));
            });
        }

        @Test
        public void emptyPredicate() {
            boolean value = RandomUtils.nextBoolean();
            when(predicate.getDatabasePredicate()).thenReturn(Optional.empty());

            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, value, true);
            Optional<BuiltCondition> result = condition.getBuiltCondition();

            assertThat(result).isEmpty();
        }

    }

    @Nested
    class GetBooleanDatabaseValue {

        @Mock
        Row row;

        @Test
        public void nullDatabaseValue() {
            boolean filterValue = RandomUtils.nextBoolean();
            when(row.isNull("bool_value")).thenReturn(true);
            when(predicate.test(anyBoolean(), eq(null))).thenReturn(true);

            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, filterValue, false);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

        @Test
        public void nonNumericBooleans() {
            boolean filterValue = RandomUtils.nextBoolean();
            boolean databaseValue = RandomUtils.nextBoolean();
            when(row.isNull("bool_value")).thenReturn(false);
            when(row.getBoolean("bool_value")).thenReturn(databaseValue);
            when(predicate.test(filterValue, databaseValue)).thenReturn(true);


            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, filterValue, false);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

        @Test
        public void numericBooleansZero() {
            boolean filterValue = RandomUtils.nextBoolean();
            byte databaseValue = 0;
            when(row.isNull("bool_value")).thenReturn(false);
            when(row.getByte("bool_value")).thenReturn(databaseValue);
            when(predicate.test(filterValue, false)).thenReturn(true);

            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, filterValue, true);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

        @Test
        public void numericBooleansOne() {
            boolean filterValue = RandomUtils.nextBoolean();
            byte databaseValue = 1;
            when(row.isNull("bool_value")).thenReturn(false);
            when(row.getByte("bool_value")).thenReturn(databaseValue);
            when(predicate.test(filterValue, true)).thenReturn(true);

            ImmutableBooleanCondition condition = ImmutableBooleanCondition.of(predicate, filterValue, true);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

    }

}