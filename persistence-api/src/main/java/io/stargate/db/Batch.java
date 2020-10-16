package io.stargate.db;

import java.util.List;

public class Batch {
  private final BatchType type;
  private final List<Statement> statements;

  public Batch(BatchType type, List<Statement> statements) {
    this.type = type;
    this.statements = statements;
  }

  public BatchType type() {
    return type;
  }

  public List<Statement> statements() {
    return statements;
  }

  public int size() {
    return statements.size();
  }
}
