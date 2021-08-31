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
package io.stargate.db.idempotency;

import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Operation;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.TimeFcts;
import org.apache.cassandra.cql3.functions.UuidFcts;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SchemaAlteringStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.TruncateStatement;
import org.apache.cassandra.cql3.statements.UseStatement;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.ListType;

import java.util.Set;

public class IdempotencyAnalyzer {
  private static final Set<Function> NON_IDEMPOTENT_FUNCTION;

  static {
    NON_IDEMPOTENT_FUNCTION =
        ImmutableSet.<Function>builder().addAll(TimeFcts.all()).addAll(UuidFcts.all()).build();
  }

  public static boolean isIdempotent(CqlStatementWrapper statement) {
    // if any of the BatchStatement is non-idempotent, return false
    if (statement.isBatchStatement()) {
      for (CqlStatementWrapper s : statement.getStatements()) {
        boolean isIdempotent = analyzeStatement(s);
        if (!isIdempotent) {
          return false;
        }
      }
    }

    return analyzeStatement(statement);
  }

  private static boolean analyzeStatement(CqlStatementWrapper statement) {
    if (statement.isSelectStatement()) {
      return true;
    }

    // Truncate, schema changes, and USE are non-idempotent.
    if (statement.isTruncate()
        || statement.isSchemaChange()
        || statement.isUse()) {
      return false;
    }

    if (statement.isModification()) {

      // check if it is a LWT
      if (statement.isLwt()) {
        return false;
      }

      // check if it is updating a Counter
      if (statement.isCounter()) {
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

      for (ColumnDefinition c : modification.updatedColumns()) {
        // check if it is updating a Counter
        if (c.type instanceof CounterColumnType) {
          return false;
        }
      }
    }

    // delete
    if (statement instanceof DeleteStatement) {
      for (ColumnDefinition c : ((DeleteStatement) statement).updatedColumns()) {
        // on a List
        if (c.type instanceof ListType) {
          return false;
        }
      }
    }
    return true;
  }
}
