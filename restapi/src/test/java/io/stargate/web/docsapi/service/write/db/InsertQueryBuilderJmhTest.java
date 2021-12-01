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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.web.docsapi.service.write.db;

import io.stargate.db.query.BoundQuery;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.builder.BuiltQuery;
import io.stargate.db.query.builder.QueryBuilder;
import io.stargate.db.schema.Schema;
import io.stargate.web.docsapi.DocsApiTestSchemaProvider;
import io.stargate.web.docsapi.service.ImmutableJsonShreddedRow;
import io.stargate.web.docsapi.service.JsonShreddedRow;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class InsertQueryBuilderJmhTest {

  private int maxDepth = 64;

  private Schema schema;

  private String keyspace;

  private String table;

  private InsertQueryBuilder queryBuilder;

  private JsonShreddedRow row;

  @Setup(Level.Trial)
  public void setup() {
    DocsApiTestSchemaProvider schemaProvider = new DocsApiTestSchemaProvider(maxDepth);
    schema = schemaProvider.getSchema();
    keyspace = schemaProvider.getKeyspace().name();
    table = schemaProvider.getTable().name();
    queryBuilder = new InsertQueryBuilder(maxDepth);
    row =
        ImmutableJsonShreddedRow.builder()
            .maxDepth(maxDepth)
            .addPath("one")
            .stringValue("whatever")
            .build();
  }

  @Benchmark
  public void buildQuery(Blackhole blackhole) {
    BuiltQuery<? extends BoundQuery> result =
        queryBuilder.buildQuery(
            () -> new QueryBuilder(schema, TypedValue.Codec.testCodec(), null), keyspace, table);
    blackhole.consume(result);
  }
}
