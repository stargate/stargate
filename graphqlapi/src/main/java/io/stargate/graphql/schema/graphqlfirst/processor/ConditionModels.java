package io.stargate.graphql.schema.graphqlfirst.processor;

import java.util.List;

/**
 * Holds all CQL conditions (WHERE and IF) that were inferred for a particular GraphQL operation.
 */
public class ConditionModels {

  private final List<ConditionModel> ifConditions;
  private final List<ConditionModel> whereConditions;

  public ConditionModels(List<ConditionModel> ifConditions, List<ConditionModel> whereConditions) {
    this.ifConditions = ifConditions;
    this.whereConditions = whereConditions;
  }

  public List<ConditionModel> getIfConditions() {
    return ifConditions;
  }

  public List<ConditionModel> getWhereConditions() {
    return whereConditions;
  }
}
