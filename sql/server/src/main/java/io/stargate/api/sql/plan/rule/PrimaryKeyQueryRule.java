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
package io.stargate.api.sql.plan.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.stargate.api.sql.plan.rel.ImmutableCondition;
import io.stargate.api.sql.plan.rel.SingleRowQuery;
import io.stargate.api.sql.plan.rel.SingleRowQuery.Condition;
import io.stargate.api.sql.schema.StorageTable;
import io.stargate.db.schema.Column;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

/**
 * A Calcite rule that converts logical table scans with a suitable filter to Cassandra queries by
 * primary key (single row).
 */
public class PrimaryKeyQueryRule extends RelRule<RelRule.Config> {

  private static final Config CONFIG =
      Config.EMPTY
          .withDescription(PrimaryKeyQueryRule.class.getSimpleName())
          .withOperandSupplier(
              b ->
                  b.operand(LogicalFilter.class)
                      .inputs(b1 -> b1.operand(LogicalTableScan.class).noInputs()))
          .as(Config.class);

  public PrimaryKeyQueryRule() {
    super(CONFIG);
  }

  private List<Condition> conditions(LogicalFilter filter, StorageTable table) {
    RexNode condition = filter.getCondition();
    RelDataType rowType = filter.getInput().getRowType();
    List<String> fieldNames = rowType.getFieldNames();

    List<RexCall> restrictions = new ArrayList<>();

    if (condition.getKind().equals(SqlKind.EQUALS)) {
      restrictions.add((RexCall) condition);
    } else if (condition.getKind().equals(SqlKind.AND)) {
      ((RexCall) condition)
          .getOperands()
          .forEach(
              o -> {
                if (o.getKind().equals(SqlKind.EQUALS)) {
                  restrictions.add((RexCall) o);
                }
              });
    }

    Map<String, Column> pkColumns =
        table.columns().stream()
            .filter(Column::isPrimaryKeyComponent)
            .collect(Collectors.toMap(Column::name, c -> c));

    Builder<Condition> conditions = ImmutableList.builder();
    for (RexCall eq : restrictions) {
      RexNode left = eq.operands.get(0);
      RexNode right = eq.operands.get(1);

      if (!left.isA(SqlKind.INPUT_REF)) {
        return null;
      }

      if (!(right.isA(SqlKind.LITERAL) || right.isA(SqlKind.DYNAMIC_PARAM))) {
        return null;
      }

      RexInputRef ref = (RexInputRef) left;
      String name = fieldNames.get(ref.getIndex());

      Column column = pkColumns.remove(name);
      if (column == null) {
        return null;
      }

      conditions.add(ImmutableCondition.builder().column(column).value(right).build());
    }

    if (!pkColumns.isEmpty()) { // some PK columns were not restricted
      return null;
    }

    return conditions.build();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final LogicalTableScan scan = call.rel(1);
    StorageTable table = scan.getTable().unwrap(StorageTable.class);

    List<Condition> conditions = conditions(filter, table);
    if (conditions == null) {
      return;
    }

    call.transformTo(SingleRowQuery.create(scan.getCluster(), scan.getTable(), conditions));
  }
}
