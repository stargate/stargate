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
package io.stargate.api.sql.server.avatica;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.db.datastore.DataStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.NoSuchStatementException;

public class Connection {

  private final DataStore dataStore;
  private final Meta.ConnectionHandle ch;
  private final AtomicInteger statementSeq = new AtomicInteger();
  private final ConcurrentMap<Integer, StatementHolder> statements = new ConcurrentHashMap<>();

  public Connection(DataStore dataStore, Meta.ConnectionHandle ch) {
    this.dataStore = dataStore;
    this.ch = ch;
  }

  private StatementHolder newStatementHandle(Function<Integer, StatementHolder> builder) {
    int id = statementSeq.incrementAndGet();
    if (id >= Integer.MAX_VALUE) {
      throw new IllegalStateException("Too many statements created");
    }

    return statements.computeIfAbsent(id, builder);
  }

  public StatementHolder newStatement(String sql) {
    return newStatementHandle(id -> StatementHolder.prepare(ch.id, id, sql, dataStore));
  }

  public StatementHolder newStatement() {
    return newStatementHandle(id -> StatementHolder.empty(ch.id, id, dataStore));
  }

  public StatementHolder statement(Meta.StatementHandle h) throws NoSuchStatementException {
    StatementHolder statement = statements.get(h.id);

    if (statement == null) {
      throw new NoSuchStatementException(h);
    }

    return statement;
  }

  public void closeStatement(Meta.StatementHandle h) {
    statements.remove(h.id);
  }

  @VisibleForTesting
  Map<Integer, StatementHolder> statements() {
    return statements;
  }
}
