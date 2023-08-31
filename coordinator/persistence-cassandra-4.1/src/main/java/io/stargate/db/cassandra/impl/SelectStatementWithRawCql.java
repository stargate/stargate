package io.stargate.db.cassandra.impl;

import org.apache.cassandra.cql3.statements.SelectStatement;

public class SelectStatementWithRawCql extends SelectStatement {
  private final String rawCQLStatement;

  public SelectStatementWithRawCql(SelectStatement statement, String rawCQLStatement) {
    super(
        statement.table,
        statement.bindVariables,
        statement.parameters,
        statement.getSelection(),
        statement.getRestrictions(),
        false,
        null,
        null,
        null,
        null);

    this.rawCQLStatement = rawCQLStatement;
  }

  public String getRawCQLStatement() {
    return rawCQLStatement;
  }
}
