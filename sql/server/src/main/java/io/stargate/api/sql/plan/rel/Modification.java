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
import io.stargate.api.sql.schema.TypeUtils;
import io.stargate.db.schema.Column;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

/** A relational operation representing Cassandra table modifications. */
public class Modification extends TableModify implements EnumerableRel {
  protected Modification(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode input,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened) {
    super(
        cluster,
        traitSet,
        table,
        catalogReader,
        input,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened);
  }

  public static RelNode create(LogicalTableModify modify, RelNode input, RelTraitSet traitSet) {
    RelOptCluster cluster = modify.getCluster();
    return new Modification(
        cluster,
        traitSet,
        modify.getTable(),
        modify.getCatalogReader(),
        input,
        modify.getOperation(),
        modify.getUpdateColumnList(),
        modify.getSourceExpressionList(),
        modify.isFlattened());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new Modification(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened());
  }

  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final Result input = implementor.visitChild(this, 0, (EnumerableRel) getInput(), Prefer.ARRAY);

    StorageTable backingTable = table.unwrap(StorageTable.class);
    RuntimeContext.Builder contextBuilder = CalciteUtils.contextBuilder(implementor);

    List<String> updateColumnList =
        getUpdateColumnList() == null ? Collections.emptyList() : getUpdateColumnList();

    List<Column> columns; // columns to be set in CQL

    String statementId;
    Operation op = getOperation();
    switch (op) {
      case INSERT:
        columns = backingTable.columns();
        statementId = contextBuilder.newUpsert(backingTable, columns);
        break;

      case UPDATE:
        // Trivial implementation: issue one CQL INSERT statement per row. Calcite will compute
        // affected rows.
        // TODO: use CQL UPDATE when possible
        columns =
            backingTable.columns().stream()
                .filter(c -> c.kind().isPrimaryKeyKind() || updateColumnList.contains(c.name()))
                .collect(Collectors.toList());
        statementId = contextBuilder.newUpsert(backingTable, columns);
        break;

      case DELETE:
        columns =
            backingTable.columns().stream()
                .filter(c -> c.kind().isPrimaryKeyKind())
                .collect(Collectors.toList());
        statementId = contextBuilder.newDelete(backingTable, columns);
        break;

      default:
        throw new UnsupportedOperationException("Unsupported modification operation: " + op);
    }

    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final JavaRowFormat format = JavaRowFormat.ARRAY;

    RelDataTypeFactory.Builder paramTypeBuilder = typeFactory.builder();
    columns.forEach(
        c ->
            paramTypeBuilder
                .add(c.name(), TypeUtils.toCalciteType(c.type()))
                .nullable(!c.kind().isPrimaryKeyKind()));
    RelDataType paramsType = paramTypeBuilder.build();
    PhysType physParamsType = PhysTypeImpl.of(typeFactory, paramsType, format);

    final BlockBuilder list = new BlockBuilder();

    Expression inputExp = list.append("input", input.block);

    RelDataType rowType = getRowType();
    RelDataType tableRowType = table.getRowType();
    final PhysType physResultType = PhysTypeImpl.of(typeFactory, rowType, format);
    final PhysType physInputType = input.physType;
    RelDataType inputRowType = physInputType.getRowType();
    ParameterExpression inputRec =
        Expressions.parameter(physInputType.getJavaRowType(), "inputRec");

    List<Expression> parameters =
        paramsType.getFieldList().stream()
            .map(
                f -> {
                  int updateIdx = updateColumnList.indexOf(f.getName());
                  RelDataTypeField inputField;
                  if (updateIdx >= 0) {
                    int inputIdx = tableRowType.getFieldCount() + updateIdx;
                    inputField = inputRowType.getFieldList().get(inputIdx);
                  } else {
                    inputField = inputRowType.getField(f.getName(), true, false);
                  }

                  if (inputField == null) {
                    throw new IllegalStateException("Input field not found: " + f.getName());
                  }

                  return physInputType.fieldReference(inputRec, inputField.getIndex());
                })
            .collect(Collectors.toList());

    Expression selectedInputExp =
        list.append(
            "selectedInput",
            Expressions.call(
                inputExp,
                BuiltInMethod.SELECT.method,
                Expressions.lambda(physParamsType.record(parameters), inputRec)));

    final Expression call =
        list.append(
            "call",
            Expressions.call(
                StatementExecutor.MODIFY,
                implementor.getRootExpression(),
                Expressions.constant(statementId),
                selectedInputExp));
    list.add(Expressions.return_(null, call));
    return implementor.result(physResultType, list.toBlock());
  }
}
