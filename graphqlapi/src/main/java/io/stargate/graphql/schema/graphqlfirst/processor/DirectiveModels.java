package io.stargate.graphql.schema.graphqlfirst.processor;

import java.util.List;

/**
 * Holds all CQL conditions (WHERE and IF) that were inferred for a particular GraphQL operation.
 */
public class DirectiveModels {

  private final List<ConditionModel> ifConditions;
  private final List<ConditionModel> whereConditions;
  private final List<IncrementModel> incrementModels;

  public DirectiveModels(
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
