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
package io.stargate.api.sql.server.postgres;

import io.stargate.api.sql.plan.PreparedSqlQuery;
import io.stargate.api.sql.plan.QueryPlanner;
import io.stargate.db.datastore.DataStore;
import org.apache.calcite.sql.parser.SqlParseException;

public class SqlParser {
  private static final QueryPlanner planner = new QueryPlanner();

  private final DataStore dataStore;

  public SqlParser(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public Statement parse(String sql) {
    Statement s = SetStatement.parse(sql);
    if (s != null) {
      return s;
    }

    try {
      PreparedSqlQuery prepared = planner.prepare(sql, dataStore, null);
      return new CalciteStatement(prepared);
    } catch (SqlParseException e) {
      throw new IllegalArgumentException(
          String.format("Unable to parse '%s', error: %s", sql, e.getMessage()), e);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
