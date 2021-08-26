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
package io.stargate.db.cassandra.impl.idempotency;

import java.util.HashSet;
import java.util.Set;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.TimeFcts;
import org.apache.cassandra.cql3.functions.UuidFcts;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.schema.ColumnMetadata;

public class IdempotencyAnalyzer {
  private static final Set<Function> NON_IDEMPOTENT_FUNCTION;

  static {
    Set<Function> allFunctions = new HashSet<>();
    allFunctions.addAll(TimeFcts.all());
    allFunctions.addAll(UuidFcts.all());
    NON_IDEMPOTENT_FUNCTION = allFunctions;
  }

  public static boolean isIdempotent(CQLStatement statement) {

    if (statement instanceof SelectStatement) {
      return true;
    }
    if (statement instanceof ModificationStatement) {
      ModificationStatement modification = (ModificationStatement) statement;

      // check if it is a LWT
      if (modification.hasIfExistCondition()
          || modification.hasIfNotExistCondition()
          || modification.hasConditions()) {
        return false;
      }

      // check if it is updating a Counter
      if (modification.isCounter()) {
        return false;
      }

      // check if contains non-idempotent function
      for (Function f : modification.getFunctions()) {
        return !NON_IDEMPOTENT_FUNCTION.contains(f);
      }

      for (ColumnMetadata c : modification.updatedColumns()) {
        // check if it is updating a List
        if (c.type instanceof ListType) {
          return false;
        }
        // check if it is updating a Counter
        if (c.type instanceof CounterColumnType) {
          return false;
        }
      }
    }

    // todo handle:
    // Batches are idempotent if their underlying statements are all
    // idempotent.
    // Truncate, schema changes, and USE are non-idempotent.
    return true;
  }
}
