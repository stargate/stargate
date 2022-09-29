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
package io.stargate.web.docsapi.service;

import io.stargate.db.datastore.ResultSet;
import io.stargate.db.query.BoundDMLQuery;
import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.RowsImpacted;
import io.stargate.web.docsapi.models.ExecutionProfile;
import io.stargate.web.docsapi.models.ImmutableExecutionProfile;
import io.stargate.web.docsapi.models.QueryInfo;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public abstract class ExecutionContext {

  public static final ExecutionContext NOOP_CONTEXT = new NoOpContext();

  public static ExecutionContext create(Boolean enabled) {
    if (Boolean.TRUE.equals(enabled)) {
      return new ProfilingContext("root");
    } else {
      return NOOP_CONTEXT;
    }
  }

  public abstract ExecutionContext nested(String description);

  /** Records information about a query when its {@link ResultSet} is retrieved. */
  public abstract void traceCqlResult(BoundQuery query, int numRows);

  /**
   * Immediately records information about a DML query that is scheduled to be executed at a later
   * time.
   */
  public abstract void traceDeferredDml(BoundQuery query);

  public abstract ExecutionProfile toProfile();

  private static class NoOpContext extends ExecutionContext {
    @Override
    public void traceCqlResult(BoundQuery query, int numRows) {
      // nop
    }

    @Override
    public void traceDeferredDml(BoundQuery query) {
      // nop
    }

    @Override
    public ExecutionContext nested(String description) {
      return this;
    }

    @Override
    public ExecutionProfile toProfile() {
      return null;
    }
  }

  private static class ProfilingContext extends ExecutionContext {
    private final Queue<ProfilingContext> steps = new ConcurrentLinkedQueue<>();
    private final Map<String, QueryInfo> executionInfoMap = new ConcurrentHashMap<>();
    private final String description;

    private ProfilingContext(String description) {
      this.description = description;
    }

    @Override
    public final ExecutionContext nested(String description) {
      ProfilingContext step = new ProfilingContext(description);
      steps.add(step);
      return step;
    }

    @Override
    public void traceCqlResult(BoundQuery query, int numRows) {
      String cql = query.source().query().queryStringForPreparation();
      executionInfoMap.merge(cql, QueryInfo.of(cql, numRows), QueryInfo::combine);
    }

    @Override
    public void traceDeferredDml(BoundQuery query) {
      int numRows = 0;

      if (query instanceof BoundDMLQuery) {
        RowsImpacted rowsImpacted = ((BoundDMLQuery) query).rowsUpdated();
        if (rowsImpacted.isKeys()) {
          numRows = rowsImpacted.asKeys().primaryKeys().size();
        }
      }

      traceCqlResult(query, numRows);
    }

    @Override
    public ExecutionProfile toProfile() {
      List<QueryInfo> queries = new ArrayList<>(executionInfoMap.values());
      // Sort by most fetched pages (i.e. execution count), then by fetched rows
      queries.sort(
          Comparator.comparing(QueryInfo::execCount)
              .thenComparing(QueryInfo::rowCount)
              .thenComparing(QueryInfo::preparedCQL)
              .reversed());

      return ImmutableExecutionProfile.builder()
          .description(description)
          .queries(queries)
          .nested(steps.stream().map(ProfilingContext::toProfile).collect(Collectors.toList()))
          .build();
    }
  }
}
