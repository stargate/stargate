package io.stargate.db.query;

import java.util.List;

public interface BoundDMLQueryWithConditions extends BoundDMLQuery {
  boolean ifExists();

  List<Condition> conditions();

  default boolean isConditional() {
    return ifExists() || !conditions().isEmpty();
  }
}
