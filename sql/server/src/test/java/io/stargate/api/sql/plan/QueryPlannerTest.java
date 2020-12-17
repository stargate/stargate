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
package io.stargate.api.sql.plan;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.stargate.api.sql.AbstractDataStoreTest;
import io.stargate.api.sql.plan.exec.StatementExecutor;
import io.stargate.db.schema.Column;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.cassandra.stargate.utils.Streams;
import org.junit.jupiter.api.Test;

public class QueryPlannerTest extends AbstractDataStoreTest {

  private final QueryPlanner executor = new QueryPlanner();

  private List<Object[]> execute(PreparedSqlQuery prepared, Object... params) {
    return Streams.of(prepared.execute(Arrays.asList(params)))
        .map(StatementExecutor::wrap)
        .collect(Collectors.toList());
  }

  private PreparedSqlQuery prepare(String sql) throws Exception {
    return executor.prepare(sql, dataStore, "test_ks");
  }

  private void withTwoRowsInTable2() {
    withQuery(table2, "SELECT x, y FROM %s")
        .returning(
            ImmutableList.of(
                ImmutableMap.of("x", 1, "y", "row_1"), ImmutableMap.of("x", 2, "y", "row_2")));
  }

  @Test
  public void simplePrepare() throws Exception {
    withTwoRowsInTable2();

    assertThat(prepare("SELECT x as z, y from test2").explain())
        .isEqualTo("FullScan(table=[[test_ks, test2]])");

    ignorePreparedExecutions();
  }

  @Test
  public void resultSetMetadata() throws Exception {
    withTwoRowsInTable2();
    List<Column> columns = prepare("SELECT x as z, y from test2").resultSetColumns();
    assertThat(columns).isNotNull();
    assertThat(columns).extracting(Column::name).containsExactly("z", "y");
    assertThat(columns).extracting(Column::type).containsExactly(Column.Type.Int, Column.Type.Text);

    ignorePreparedExecutions();
  }

  @Test
  public void simpleSelect() throws Exception {
    withTwoRowsInTable2();
    PreparedSqlQuery prepared = prepare("SELECT x as z, y from test2");
    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(1, 2);
    assertThat(result).extracting(a -> a[1]).containsExactly("row_1", "row_2");
  }

  @Test
  public void selectWithPostgresBindMarkers() throws Exception {
    withTwoRowsInTable2();
    PreparedSqlQuery prepared = prepare("SELECT x as z, y from test2 where y = $1");
    List<Object[]> result = execute(prepared, "row_2");
    assertThat(result).extracting(a -> a[0]).containsExactly(2);
    assertThat(result).extracting(a -> a[1]).containsExactly("row_2");
  }

  @Test
  public void simpleInsert() throws Exception {
    withQuery(table2, "INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)", 11, "aaa")
        .returningNothing();

    PreparedSqlQuery prepared = prepare("INSERT INTO test2 (x, y) VALUES (11, 'aaa')");

    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void insertWithBindMarkers() throws Exception {
    withQuery(table2, "INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)", 11, "aaa")
        .returningNothing();

    PreparedSqlQuery prepared = prepare("INSERT INTO test2 (x, y) VALUES (?, ?)");

    List<Object[]> result = execute(prepared, 11, "aaa");
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void insertWithPostgresBindMarkers() throws Exception {
    withQuery(table2, "INSERT INTO test_ks.test2 (x, y) VALUES (?, ?)", 11, "aaa")
        .returningNothing();

    PreparedSqlQuery prepared = prepare("INSERT INTO test2 (x, y) VALUES ($2, $1)");

    List<Object[]> result = execute(prepared, "aaa", 11);
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void simpleUpdate() throws Exception {
    withTwoRowsInTable2();
    withQuery(table2, "INSERT INTO %s (x, y) VALUES (?, ?)", 1, "bbb").returningNothing();

    PreparedSqlQuery prepared = prepare("UPDATE test2 SET y = 'bbb' WHERE y = 'row_1'");

    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void updateWithPostgresBindMarkers() throws Exception {
    withTwoRowsInTable2();
    withQuery(table2, "INSERT INTO %s (x, y) VALUES (?, ?)", 1, "bbb").returningNothing();

    PreparedSqlQuery prepared = prepare("UPDATE test2 SET y = $2 WHERE y = $1");

    List<Object[]> result = execute(prepared, "row_1", "bbb");
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void simpleDelete() throws Exception {
    withTwoRowsInTable2();
    withQuery(table2, "DELETE FROM %s WHERE x = ?", 2).returningNothing();

    PreparedSqlQuery prepared = prepare("DELETE FROM test2 WHERE y = 'row_2'");

    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
  }

  @Test
  public void joinSingleColumnToMultiColumnTable() throws Exception {
    withTwoRowsInTable2();
    withQuery(table1, "SELECT a FROM %s")
        .returning(ImmutableList.of(ImmutableMap.of("a", 20), ImmutableMap.of("a", 10)));

    PreparedSqlQuery prepared = prepare("SELECT a, x, y from test1, test2 ORDER BY x, a");
    List<Object[]> result = execute(prepared);
    assertThat(result).extracting(a -> a[0]).containsExactly(10, 20, 10, 20);
    assertThat(result).extracting(a -> a[1]).containsExactly(1, 1, 2, 2);
    assertThat(result).extracting(a -> a[2]).containsExactly("row_1", "row_1", "row_2", "row_2");
  }

  @Test
  public void selectByPrimaryKey() throws Exception {
    withQuery(table2, "SELECT x, y FROM %s WHERE x = ?", 2)
        .returning(ImmutableList.of(ImmutableMap.of("x", 2, "y", "row_2")));

    PreparedSqlQuery prepared = prepare("SELECT * FROM test2 where x = ?");

    assertThat(prepared.explain()).contains("SingleRowQuery(table=[[test_ks, test2]], x=[?0])");

    List<Object[]> result = execute(prepared, 2);
    assertThat(result).extracting(a -> a[0]).containsExactly(2);
    assertThat(result).extracting(a -> a[1]).containsExactly("row_2");

    withQuery(table2, "SELECT x, y FROM %s WHERE x = ?", 123)
        .returning(ImmutableList.of(ImmutableMap.of("x", 123, "y", "row_123")));

    result = execute(prepare("SELECT * FROM test2 where x = 123"));
    assertThat(result).extracting(a -> a[0]).containsExactly(123);
    assertThat(result).extracting(a -> a[1]).containsExactly("row_123");
  }

  @Test
  public void selectByCompositePrimaryKey() throws Exception {
    withQuery(table2a, "SELECT x1, x2, y FROM %s WHERE x1 = ? AND x2 = ?", 1, 2)
        .returning(ImmutableList.of(ImmutableMap.of("x1", 1, "x2", 2, "y", "row12")));

    PreparedSqlQuery prepared = prepare("SELECT * FROM test2a where x1 = ? and x2 = ?");

    assertThat(prepared.explain())
        .contains("SingleRowQuery(table=[[test_ks, test2a]], x1=[?0], x2=[?1])");

    List<Object[]> result = execute(prepared, 1, 2);
    assertThat(result).extracting(a -> a[0]).containsExactly(1);
    assertThat(result).extracting(a -> a[1]).containsExactly(2);
    assertThat(result).extracting(a -> a[2]).containsExactly("row12");

    withQuery(table2a, "SELECT x1, x2, y FROM %s WHERE x2 = ? AND x1 = ?", 22, 11)
        .returning(ImmutableList.of(ImmutableMap.of("x1", 11, "x2", 22, "y", "row1122")));

    result = execute(prepare("SELECT x2, y, x1 FROM test2a where x2 = 22 AND x1 = 11"));
    assertThat(result).extracting(a -> a[0]).containsExactly(22);
    assertThat(result).extracting(a -> a[1]).containsExactly("row1122");
    assertThat(result).extracting(a -> a[2]).containsExactly(11);
  }
}
