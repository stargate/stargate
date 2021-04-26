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
import io.stargate.web.docsapi.service.query.predicate.impl.ExistsPredicate;
import io.stargate.web.docsapi.service.query.predicate.impl.InPredicate;
import io.stargate.web.docsapi.service.query.predicate.impl.NotInPredicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AnyValueConditionTest {


    // set of simple int test in order to confirm with existing predicates
    @Nested
    class Integration {

        @Mock
        Row row;

        @BeforeEach
        public void initRow() {
            when(row.isNull("bool_value")).thenReturn(true);
            when(row.isNull("dbl_value")).thenReturn(true);
            when(row.isNull("text_value")).thenReturn(true);
        }

        @Test
        public void existsPredicateNotMatched() {
            AnyValueCondition<Boolean> condition = ImmutableAnyValueCondition.of(ExistsPredicate.of(), true, false);
            boolean result = condition.test(row);

            assertThat(result).isFalse();
        }

        @Test
        public void existsPredicateMatch() {
            String findMe = "find-me";
            when(row.isNull("text_value")).thenReturn(false);
            when(row.getString("text_value")).thenReturn(findMe);

            AnyValueCondition<Boolean> condition = ImmutableAnyValueCondition.of(ExistsPredicate.of(), true, false);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

        @Test
        public void inPredicateNotMatched() {
            String findMe = "find-me";
            List<?> queryValue = Collections.singletonList(findMe);

            AnyValueCondition<List<?>> condition = ImmutableAnyValueCondition.of(InPredicate.of(), queryValue, false);
            boolean result = condition.test(row);

            assertThat(result).isFalse();
        }

        @Test
        public void inPredicateMatch() {
            String findMe = "find-me";
            List<?> queryValue = Collections.singletonList(findMe);
            when(row.isNull("text_value")).thenReturn(false);
            when(row.getString("text_value")).thenReturn(findMe);

            AnyValueCondition<List<?>> condition = ImmutableAnyValueCondition.of(InPredicate.of(), queryValue, false);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

        @Test
        public void notInPredicateEmptyRow() {
            String findMe = "find-me";
            List<?> queryValue = Collections.singletonList(findMe);

            AnyValueCondition<List<?>> condition = ImmutableAnyValueCondition.of(NotInPredicate.of(), queryValue, false);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

        @Test
        public void notInPredicateNotMatch() {
            String findMe = "find-me";
            List<?> queryValue = Collections.singletonList(findMe);
            when(row.isNull("text_value")).thenReturn(false);
            when(row.getString("text_value")).thenReturn(findMe);

            AnyValueCondition<List<?>> condition = ImmutableAnyValueCondition.of(NotInPredicate.of(), queryValue, false);
            boolean result = condition.test(row);

            assertThat(result).isFalse();
        }

        @Test
        public void notInPredicateMatch() {
            String findMe = "find-me";
            List<?> queryValue = Collections.singletonList(findMe);
            when(row.isNull("text_value")).thenReturn(false);
            when(row.getString("text_value")).thenReturn("something");

            AnyValueCondition<List<?>> condition = ImmutableAnyValueCondition.of(NotInPredicate.of(), queryValue, false);
            boolean result = condition.test(row);

            assertThat(result).isTrue();
        }

    }

}