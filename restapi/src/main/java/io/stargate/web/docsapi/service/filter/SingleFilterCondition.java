package io.stargate.web.docsapi.service.filter;

import io.stargate.web.docsapi.exception.SchemalessRequestException;
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
      throw new SchemalessRequestException(
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
      throw new SchemalessRequestException(
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
      throw new SchemalessRequestException(
          String.format(
              "Invalid operator: %s, valid operators are: %s", opStr, FilterOp.allRawValues()));
    }
    this.textValue = null;
    this.doubleValue = null;
    this.booleanValue = booleanValue;
  }

  public FilterOp getFilterOp() {
    return op;
  }

  public List<String> getPath() {
    return path;
  }

  public String getPathString() {
    return String.join(".", path);
  }

  public String getField() {
    return field;
  }

  public String getFullFieldPath() {
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

  public Boolean getBooleanValue() {
    return booleanValue;
  }

  public String getTextValue() {
    return textValue;
  }

  public Double getDoubleValue() {
    return doubleValue;
  }

  public String toString() {
    return String.format("SingleFilterCondition{%s, %s, %s, %s}", path, field, op, getValue());
  }
}
