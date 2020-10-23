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

import io.stargate.api.sql.plan.rel.FullScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

/** A Calcite rule that converts logical table scans to full Cassandra table scans. */
public class FullScanRule extends RelRule<RelRule.Config> {

  private static final Config CONFIG =
      Config.EMPTY
          .withDescription(FullScanRule.class.getSimpleName())
          .withOperandSupplier(b -> b.operand(LogicalTableScan.class).noInputs())
          .as(Config.class);

  public FullScanRule() {
    super(CONFIG);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalTableScan scan = call.rel(0);
    call.transformTo(FullScan.create(scan.getCluster(), scan.getTable()));
  }
}
