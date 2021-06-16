package io.stargate.graphql.schema.graphqlfirst.processor;

public class IncrementModel {
  private final FieldModel field;
  private final boolean prepend;
  private final String argumentName;

  public IncrementModel(FieldModel field, boolean prepend, String argumentName) {
    this.field = field;
    this.prepend = prepend;
    this.argumentName = argumentName;
  }

  public FieldModel getField() {
    return field;
  }

  public boolean isPrepend() {
    return prepend;
  }

  /** The name of the argument that will contain the value to bind. */
  public String getArgumentName() {
    return argumentName;
  }
}
