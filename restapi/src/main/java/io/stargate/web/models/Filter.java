package io.stargate.web.models;

import java.util.List;

public class Filter {
  public enum Operator {
    eq,
    notEq,
    gt,
    gte,
    lt,
    lte,
    in,
  }

  String columnName;
  Operator operator;
  List<Object> value;

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public Operator getOperator() {
    return operator;
  }

  public void setOperator(Operator operator) {
    this.operator = operator;
  }

  public List<Object> getValue() {
    return value;
  }

  public void setValue(List<Object> value) {
    this.value = value;
  }
}