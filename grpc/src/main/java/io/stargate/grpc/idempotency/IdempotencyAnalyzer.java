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

import io.stargate.proto.QueryOuterClass;
import java.util.Arrays;
import java.util.List;

public class IdempotencyAnalyzer {
  private static final List<String> nonIdempotentFunctions = Arrays.asList("now()", "uuid()");

  public static boolean isIdempotent(List<QueryOuterClass.BatchQuery> queries) {
    for (QueryOuterClass.BatchQuery query : queries) {
      boolean idempotent = isIdempotent(query.getCql());
      // if ANY query within a batch is non-idempotent, then the whole batch is non-idempotent
      if (!idempotent) {
        return false;
      }
    }
    return true;
  }

  public static boolean isIdempotent(String query) {
    // analyze only mutations
    if (query.contains("SELECT")) {
      return true;
    }

    // check if it is updating a Collection or Counter
    if (query.contains("SET") && query.contains("+") && query.contains("WHERE")) {
      return false;
    }

    // check if it is an LWT
    if (query.contains("SET") && query.contains("WHERE") && query.contains("IF")) {
      return false;
    }

    // check if contains non-idempotent function
    for (String nonIdempotentFunction : nonIdempotentFunctions) {
      if (query.contains(nonIdempotentFunction)) {
        return false;
      }
    }

    return true;
  }
}
