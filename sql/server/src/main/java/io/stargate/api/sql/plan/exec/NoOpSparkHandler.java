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
package io.stargate.api.sql.plan.exec;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;

public class NoOpSparkHandler implements CalcitePrepare.SparkHandler {
  @Override
  public boolean enabled() {
    return false;
  }

  @Override
  public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel, boolean restructure) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerRules(RuleSetBuilder builder) {
    // nop
  }

  @Override
  public ArrayBindable compile(ClassDeclaration expr, String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object sparkContext() {
    throw new UnsupportedOperationException();
  }
}
