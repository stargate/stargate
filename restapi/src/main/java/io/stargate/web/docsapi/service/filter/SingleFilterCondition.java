package io.stargate.web.docsapi.service.filter;

import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import java.util.List;

public class SingleFilterCondition implements FilterCondition {
  private List<String> path;
  private String field;
  private FilterOp op;
  private String textValue;
  private Double doubleValue;
  private Boolean booleanValue;

  public SingleFilterCondition(List<String> fieldPath, String opStr, String textValue) {
    this.path = fieldPath.subList(0, fieldPath.size() - 1);
    this.field = fieldPath.get(fieldPath.size() - 1);
    try {
      this.op = FilterOp.valueOf(opStr.toUpperCase().substring(1));
    } catch (IllegalArgumentException e) {
      throw new DocumentAPIRequestException(
          String.format(
              "Invalid operator: %s, valid operators are: %s", opStr, FilterOp.allRawValues()));
    }
    this.textValue = textValue;
    this.doubleValue = null;
    this.booleanValue = null;
  }

  public SingleFilterCondition(List<String> fieldPath, String opStr, Double doubleValue) {
    this.path = fieldPath.subList(0, fieldPath.size() - 1);
    this.field = fieldPath.get(fieldPath.size() - 1);
    try {
      this.op = FilterOp.valueOf(opStr.toUpperCase().substring(1));
    } catch (IllegalArgumentException e) {
      throw new DocumentAPIRequestException(
          String.format(
              "Invalid operator: %s, valid operators are: %s", opStr, FilterOp.allRawValues()));
    }
    this.textValue = null;
    this.doubleValue = doubleValue;
    this.booleanValue = null;
  }

  public SingleFilterCondition(List<String> fieldPath, String opStr, Boolean booleanValue) {
    this.path = fieldPath.subList(0, fieldPath.size() - 1);
    this.field = fieldPath.get(fieldPath.size() - 1);
    try {
      this.op = FilterOp.valueOf(opStr.toUpperCase().substring(1));
    } catch (IllegalArgumentException e) {
      throw new DocumentAPIRequestException(
          String.format(
              "Invalid operator: %s, valid operators are: %s", opStr, FilterOp.allRawValues()));
    }
    this.textValue = null;
    this.doubleValue = null;
    this.booleanValue = booleanValue;
  }

  @Override
  public FilterOp getFilterOp() {
    return op;
  }

  @Override
  public List<String> getPath() {
    return path;
  }

  @Override
  public String getPathString() {
    return String.join(".", path);
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public String getFullFieldPath() {
    if (getPathString().isEmpty()) return field;
    return getPathString() + "." + field;
  }

  public String getValueColumnName() {
    if (doubleValue != null) {
      return "dbl_value";
    } else if (booleanValue != null) {
      return "bool_value";
    } else {
      return "text_value";
    }
  }

  @Override
  public Object getValue() {
    if (doubleValue != null) {
      return doubleValue;
    } else if (booleanValue != null) {
      return booleanValue;
    } else if (textValue != null) {
      return textValue;
    }
    return null;
  }

  public Object getValue(boolean treatBooleansAsNumeric) {
    if (doubleValue != null) {
      return doubleValue;
    } else if (booleanValue != null) {
      if (treatBooleansAsNumeric) return booleanValue ? 1 : 0;
      return booleanValue;
    } else if (textValue != null) {
      return textValue;
    }
    return null;
  }

  public Boolean getBooleanValue() {
    return booleanValue;
  }

  public String getTextValue() {
    return textValue;
  }

  public Double getDoubleValue() {
    return doubleValue;
  }

  @Override
  public String toString() {
    return String.format("SingleFilterCondition{%s, %s, %s, %s}", path, field, op, getValue());
  }
}
