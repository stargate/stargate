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
