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

import io.reactivex.Flowable;
import io.stargate.api.sql.plan.PreparedSqlQuery;
import io.stargate.api.sql.server.postgres.msg.Bind;
import io.stargate.api.sql.server.postgres.msg.CommandComplete;
import io.stargate.api.sql.server.postgres.msg.PGServerMessage;
import java.util.concurrent.atomic.AtomicLong;

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
  public Portal bind(Bind bind) {
    if (prepared.isDml()) {
      return new DmlPortal(bind);
    } else {
      return new SelectPortal(bind);
    }
  }

  @Override
  public String toString() {
    return prepared.kind().toString();
  }

  private class DmlPortal extends Portal {

    public DmlPortal(Bind bind) {
      super(CalciteStatement.this, bind);
    }

    @Override
    protected boolean hasResultSet() {
      return false;
    }

    @Override
    public Flowable<PGServerMessage> execute(Connection connection) {
      Iterable<Object> rows = prepared.execute(parameters());

      // expect one row with the update count
      Object next = rows.iterator().next();
      long count = ((Number) next).longValue();
      return Flowable.just(CommandComplete.forDml(prepared.kind(), count));
    }
  }

  private class SelectPortal extends Portal {

    public SelectPortal(Bind bind) {
      super(CalciteStatement.this, bind);
    }

    @Override
    protected boolean hasResultSet() {
      return true;
    }

    @Override
    public Flowable<PGServerMessage> execute(Connection connection) {
      Iterable<Object> rows = prepared.execute(parameters());

      AtomicLong count = new AtomicLong();
      return Flowable.fromIterable(rows)
          .map(
              row -> {
                count.incrementAndGet();
                return toDataRow(row);
              })
          .concatWith(Flowable.defer(() -> Flowable.just(CommandComplete.forSelect(count.get()))));
    }
  }
}
