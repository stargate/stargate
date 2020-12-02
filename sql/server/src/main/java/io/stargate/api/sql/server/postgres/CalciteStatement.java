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
import java.util.List;

public class CalciteStatement extends Statement {

  private final PreparedSqlQuery prepared;

  public CalciteStatement(PreparedSqlQuery prepared) {
    this.prepared = prepared;
  }

  @Override
  public PreparedSqlQuery prepared() {
    return prepared;
  }

  @Override
  public Iterable<Object> execute(Connection connection, List<?> parameters) {
    return prepared.execute(parameters);
  }

  @Override
  public String toString() {
    return prepared.kind().toString();
  }
}
