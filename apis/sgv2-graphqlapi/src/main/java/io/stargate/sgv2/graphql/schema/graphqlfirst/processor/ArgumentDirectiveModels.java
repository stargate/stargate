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
package io.stargate.sgv2.graphql.schema.graphqlfirst.processor;

import java.util.List;

/**
 * Holds all CQL arguments including conditions (WHERE and IF) that were inferred for a particular
 * GraphQL operation and increments.
 */
public class ArgumentDirectiveModels {

  private final List<ConditionModel> ifConditions;
  private final List<ConditionModel> whereConditions;
  private final List<IncrementModel> incrementModels;

  public ArgumentDirectiveModels(
      List<ConditionModel> ifConditions,
      List<ConditionModel> whereConditions,
      List<IncrementModel> incrementModels) {
    this.ifConditions = ifConditions;
    this.whereConditions = whereConditions;
    this.incrementModels = incrementModels;
  }

  public List<ConditionModel> getIfConditions() {
    return ifConditions;
  }

  public List<ConditionModel> getWhereConditions() {
    return whereConditions;
  }

  public List<IncrementModel> getIncrementModels() {
    return incrementModels;
  }
}
