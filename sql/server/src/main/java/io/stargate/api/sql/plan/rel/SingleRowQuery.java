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
import io.stargate.db.schema.Column;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.immutables.value.Value;

/** A relational operation representing a Cassandra query by primary key. */
public class SingleRowQuery extends TableScan implements EnumerableRel {

  private final List<Condition> conditions;

  private SingleRowQuery(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Class elementType,
      List<Condition> conditions) {
    super(cluster, traitSet, Collections.emptyList(), table);
    this.conditions = conditions;
  }

  public static SingleRowQuery create(
      RelOptCluster cluster, RelOptTable relOptTable, List<Condition> conditions) {
    final Table table = relOptTable.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(table);
    final RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
    return new SingleRowQuery(cluster, traitSet, relOptTable, elementType, conditions);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeCost(1, 1, 1);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter writer = super.explainTerms(pw);
    conditions.forEach(c -> writer.item(c.column().name(), c.value()));
    return writer;
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    StorageTable backingTable = table.unwrap(StorageTable.class);

    RuntimeContext.Builder contextBuilder = CalciteUtils.contextBuilder(implementor);
    List<Column> keys = conditions.stream().map(Condition::column).collect(Collectors.toList());
    String statementId = contextBuilder.newSelectByKey(backingTable, keys);

    JavaTypeFactory typeFactory = implementor.getTypeFactory();

    RelDataType emptyType = typeFactory.createStructType(Collections.emptyList());
    RexProgramBuilder pb = new RexProgramBuilder(emptyType, getCluster().getRexBuilder());
    conditions.forEach(c -> pb.addProject(c.value(), null));

    RelDataTypeFactory.Builder paramsRowTypeBuilder = typeFactory.builder();
    conditions.forEach(
        c -> paramsRowTypeBuilder.add(c.column().name(), c.value().getType()).nullable(false));

    PhysType physParamsType =
        PhysTypeImpl.of(typeFactory, paramsRowTypeBuilder.build(), JavaRowFormat.ARRAY);

    final BlockBuilder list = new BlockBuilder();
    List<Expression> inputs =
        RexToLixTranslator.translateProjects(
            pb.getProgram(),
            typeFactory,
            SqlConformanceEnum.DEFAULT,
            list,
            physParamsType,
            DataContext.ROOT,
            new NoInput(),
            (s -> new NoInput()));

    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), rowType, JavaRowFormat.ARRAY);
    final Expression call =
        list.append(
            "call",
            Expressions.call(
                StatementExecutor.SELECT,
                implementor.getRootExpression(),
                Expressions.constant(statementId),
                physParamsType.record(inputs)));
    list.add(Expressions.return_(null, call));
    return implementor.result(physType, list.toBlock());
  }

  @Value.Immutable
  public abstract static class Condition {

    public abstract Column column();

    public abstract RexNode value();
  }

  private static final class NoInput implements InputGetter {

    @Override
    public Expression field(BlockBuilder blockBuilder, int i, Type type) {
      throw new IllegalArgumentException("No input available");
    }
  }
}
