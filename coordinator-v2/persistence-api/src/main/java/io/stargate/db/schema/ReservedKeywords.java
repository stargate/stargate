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
package io.stargate.db.schema;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

public final class ReservedKeywords {
  @VisibleForTesting
  static final String[] reservedKeywords =
      new String[] {
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "ENTRIES",
        "FULL",
        "INSERT",
        "UPDATE",
        "WITH",
        "LIMIT",
        "USING",
        "USE",
        "SET",
        "BEGIN",
        "UNLOGGED",
        "BATCH",
        "APPLY",
        "TRUNCATE",
        "DELETE",
        "IN",
        "CREATE",
        "KEYSPACE",
        "SCHEMA",
        "COLUMNFAMILY",
        "TABLE",
        "MATERIALIZED",
        "VIEW",
        "INDEX",
        "ON",
        "TO",
        "DROP",
        "PRIMARY",
        "INTO",
        "ALTER",
        "RENAME",
        "ADD",
        "ORDER",
        "BY",
        "ASC",
        "DESC",
        "ALLOW",
        "IF",
        "IS",
        "GRANT",
        "OF",
        "REVOKE",
        "MODIFY",
        "AUTHORIZE",
        "DESCRIBE",
        "EXECUTE",
        "NORECURSIVE",
        "TOKEN",
        "NULL",
        "NOT",
        "NAN",
        "INFINITY",
        "OR",
        "REPLACE",
        "DEFAULT",
        "UNSET",
        "MBEAN",
        "MBEANS",
        "FOR",
        "RESTRICT",
        "UNRESTRICT"
      };

  private static final Set<String> reservedSet =
      new CopyOnWriteArraySet<>(Arrays.asList(reservedKeywords));

  /** This class must not be instantiated as it only contains static methods. */
  private ReservedKeywords() {}

  public static boolean isReserved(String text) {
    return reservedSet.contains(text.toUpperCase());
  }
}
