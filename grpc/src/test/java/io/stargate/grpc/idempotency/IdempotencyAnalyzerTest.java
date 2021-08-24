/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.stargate.proto.QueryOuterClass;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class IdempotencyAnalyzerTest {
  @ParameterizedTest
  @MethodSource("queriesToInferIdempotence")
  public void validateIdempotencyOfQueries(String cqlQuery, boolean isIdempotent) {
    assertThat(IdempotencyAnalyzer.isIdempotent(cqlQuery)).isEqualTo(isIdempotent);
  }

  @Test
  public void validateIdempotencyOfAllIdempotentBatchQueries() {
    QueryOuterClass.BatchQuery firstIdempotentQuery =
        QueryOuterClass.BatchQuery.newBuilder()
            .setCql("update my_table SET list_col = [1] WHERE pk = 1")
            .build();
    QueryOuterClass.BatchQuery secondIdempotentQuery =
        QueryOuterClass.BatchQuery.newBuilder()
            .setCql(
                "SELECT user_name, password FROM user_by_session WHERE session_token = '1000001'")
            .build();

    assertThat(
            IdempotencyAnalyzer.isIdempotent(
                Arrays.asList(firstIdempotentQuery, secondIdempotentQuery)))
        .isTrue();
  }

  @Test
  public void validateIdempotencyOfNotAllIdempotentBatchQueries() {
    QueryOuterClass.BatchQuery firstIdempotentQuery =
        QueryOuterClass.BatchQuery.newBuilder()
            .setCql("update my_table SET list_col = [1] WHERE pk = 1")
            .build();
    QueryOuterClass.BatchQuery secondNonIdempotentQuery =
        QueryOuterClass.BatchQuery.newBuilder()
            .setCql("UPDATE my_table SET v = 4 WHERE k = 1 IF v = 1")
            .build();

    assertThat(
            IdempotencyAnalyzer.isIdempotent(
                Arrays.asList(firstIdempotentQuery, secondNonIdempotentQuery)))
        .isFalse();
  }

  public static Stream<Arguments> queriesToInferIdempotence() {
    return Stream.of(
        arguments("update my_table SET list_col = [1] WHERE pk = 1", true), // collection
        arguments(
            "UPDATE my_table SET list_col = [1] + list_col WHERE pk = 1", false), // append to list
        arguments(
            "UPDATE my_table SET list_col = list_col + [1] where pk = 1", false), // prepend to list
        arguments(
            "DELETE desserts[1] FROM table WHERE where food = 'steak';", false), // delete from list
        arguments("UPDATE my_table SET v = now() WHERE pk = 1", false), // using now() function
        arguments("UPDATE my_table SET v = uuid() WHERE pk = 1", false), // using uuid() function
        arguments(
            "UPDATE my_table SET counter_value = counter_value + 1 WHERE pk = 1", false), // counter
        arguments("UPDATE my_table SET v = 4 WHERE k = 1 IF v = 1", false), // transaction
        arguments("UPDATE my_table SET v = 4 WHERE k = 1 if EXISTS", false), // transaction
        arguments(
            "UPDATE my_table SET teams[1] = 'V' WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2",
            true), // update map
        arguments(
            "DELETE sponsorship ['v'] FROM my_table WHERE race_name = 'v';",
            true), // delete from map
        arguments(
            "DELETE sponsorship['v'] FROM my_table WHERE race_name = 'v';",
            false), // delete from map malformed
        arguments(
            "UPDATE my_table SET teams = teams + {'T'} WHERE id = 5b6962dd-3f90-4c93-8f61-eabfa4a803e2",
            true), // add to set
        arguments(
            "UPDATE Food_menu SET Menu_items = Menu_items - { 'Banana'} WHERE Cafe_id = 7801;",
            true), // remove from set
        arguments(
            "DELETE sponsorship FROM cycling.races WHERE race_name = 'v'",
            true), // delete all from a set
        arguments(
            "UPDATE my_table = '2000-12-12' WHERE id = 220844bf-4860-49d6-9a4b-6b5d3a79cbfb",
            true), // UDT
        arguments(
            "UPDATE my_table SET firstname = 'Marianne, lastname = 'VOS WHERE id = 88b8fd18-b1ed-4e96-bf79-4280797cba80",
            true), // standard update
        arguments(
            "SELECT user_name, password FROM user_by_session WHERE session_token = '1000001'",
            true), // select
        arguments(
            "INSERT INTO cycling.cyclist_name (id, lastname, firstname) VALUES (5b6962dd-3f90-4c93-8f61-eabfa4a803e2, 'VOS','Marianne');",
            true) // insert
        );
  }
}
