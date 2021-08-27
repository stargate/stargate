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
package io.stargate.db.dse.impl.idempotency;

import java.util.HashSet;
import java.util.Set;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.TimeFcts;
import org.apache.cassandra.cql3.functions.UuidFcts;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.SchemaTransformation;

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

    // Truncate, schema changes, and USE are non-idempotent.
    if (statement instanceof TruncateStatement
        || statement instanceof SchemaTransformation
        || statement instanceof UseStatement) {
      return false;
    }

    // if any of the BatchStatement is non-idempotent, return false
    if (statement instanceof BatchStatement) {
      BatchStatement batchStatement = (BatchStatement) statement;
      for (ModificationStatement s : batchStatement.getStatements()) {
        boolean isIdempotent = analyzeModificationStatement(s);
        if (!isIdempotent) {
          return false;
        }
      }
    }

    return analyzeModificationStatement(statement);
  }

  private static boolean analyzeModificationStatement(CQLStatement statement) {
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
        if (NON_IDEMPOTENT_FUNCTION.contains(f)) {
          return false;
        }
      }

      for (Operation operation : modification.allOperations()) {
        // check if it is prepending/appending a List
        if (operation instanceof Lists.Prepender || operation instanceof Lists.Appender) {
          return false;
        }
      }

      for (ColumnMetadata c : modification.updatedColumns()) {
        // check if it is updating a Counter
        if (c.type instanceof CounterColumnType) {
          return false;
        }
      }
    }

    // delete
    if (statement instanceof DeleteStatement) {
      for (ColumnMetadata c : ((DeleteStatement) statement).updatedColumns()) {
        // on a List
        if (c.type instanceof ListType) {
          return false;
        }
      }
    }

    return true;
  }
}
