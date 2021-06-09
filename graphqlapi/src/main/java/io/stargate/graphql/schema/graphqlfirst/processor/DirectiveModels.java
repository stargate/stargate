package io.stargate.graphql.schema.graphqlfirst.processor;

import java.util.List;

/**
 * Holds all CQL conditions (WHERE and IF) that were inferred for a particular GraphQL operation.
 */
public class DirectiveModels {

  private final List<ConditionModel> ifConditions;
  private final List<ConditionModel> whereConditions;
  private final List<IncrementModel> incrementModel;

  public DirectiveModels(
      List<ConditionModel> ifConditions,
      List<ConditionModel> whereConditions,
      List<IncrementModel> incrementModel) {
    this.ifConditions = ifConditions;
    this.whereConditions = whereConditions;
    this.incrementModel = incrementModel;
  }

  public List<ConditionModel> getIfConditions() {
    return ifConditions;
  }

  public List<ConditionModel> getWhereConditions() {
    return whereConditions;
  }

  public List<IncrementModel> getIncrementModel() {
    return incrementModel;
  }
}
