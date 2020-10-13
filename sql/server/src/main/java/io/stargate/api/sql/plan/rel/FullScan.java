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
package io.stargate.api.sql.plan.rel;

import io.stargate.api.sql.plan.CalciteUtils;
import io.stargate.api.sql.plan.exec.RuntimeContext;
import io.stargate.api.sql.plan.exec.StatementExecutor;
import io.stargate.api.sql.schema.StorageTable;
import java.util.Collections;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.schema.Table;

/** Relational operation representing a full Cassandra table scan. */
public class FullScan extends TableScan implements EnumerableRel {
  private FullScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, Collections.emptyList(), table);
  }

  public static FullScan create(RelOptCluster cluster, RelOptTable relOptTable) {
    final Table table = relOptTable.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(table);
    final RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
    return new FullScan(cluster, traitSet, relOptTable);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeCost(estimateRowCount(mq), 22000, 33000);
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    StorageTable backingTable = table.unwrap(StorageTable.class);

    RuntimeContext.Builder contextBuilder = CalciteUtils.contextBuilder(implementor);
    String statementId = contextBuilder.newFullScan(backingTable);

    final BlockBuilder list = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType, JavaRowFormat.ARRAY);
    final Expression call =
        list.append(
            "call",
            Expressions.call(
                StatementExecutor.SELECT,
                implementor.getRootExpression(),
                Expressions.constant(statementId)));
    list.add(Expressions.return_(null, call));
    return implementor.result(physType, list.toBlock());
  }
}
