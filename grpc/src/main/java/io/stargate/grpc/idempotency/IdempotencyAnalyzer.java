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
import java.util.regex.Pattern;

public class IdempotencyAnalyzer {
  private static final List<String> NON_IDEMPOTENT_FUNCTIONS = Arrays.asList("now()", "uuid()");
  private static final Pattern DELETE_FROM_MAP_PATTERN =
      Pattern.compile("[a-zA-Z0-9 ] \\["); // any table name followed by white space and '['

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
    String q = query.toUpperCase();
    // analyze only mutations
    if (q.contains("SELECT")) {
      return true;
    }

    // check if it is an update/insert to a set or map
    if (q.contains("SET")
        && q.contains("+")
        && q.contains("{")
        && q.contains("}")
        && q.contains("WHERE")) {
      return true;
    }
    // check if it is updating (prepend or append) a Collection or Counter
    if (q.contains("SET") && q.contains("+") && q.contains("WHERE")) {
      return false;
    }

    // check if it is deleting from a Map
    if (q.contains("DELETE") && DELETE_FROM_MAP_PATTERN.matcher(q).find()) {
      return true;
    }

    // check if it is deleting from a list
    if (q.contains("DELETE") && q.contains("[") && q.contains("]") && q.contains("WHERE")) {
      return false;
    }

    // check if it is an LWT
    if (q.contains("SET") && q.contains("WHERE") && q.contains("IF")) {
      return false;
    }
    // check if contains non-idempotent function
    for (String nonIdempotentFunction : NON_IDEMPOTENT_FUNCTIONS) {
      if (q.contains(nonIdempotentFunction.toUpperCase())) {
        return false;
      }
    }

    return true;
  }
}
