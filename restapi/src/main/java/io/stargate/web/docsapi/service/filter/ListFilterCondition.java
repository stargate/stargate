package io.stargate.web.docsapi.service.filter;

import io.stargate.web.docsapi.exception.DocumentAPIRequestException;
import java.util.List;

/** Represents a filter condition where the value is a List of elements. e.g. the `in` operator. */
public class ListFilterCondition implements FilterCondition {
  private List<String> path;
  private String field;
  private FilterOp op;
  private List<Object> listValue;

  public ListFilterCondition(List<String> fieldPath, String opStr, List<Object> listValue) {
    this.path = fieldPath.subList(0, fieldPath.size() - 1);
    this.field = fieldPath.get(fieldPath.size() - 1);
    try {
      this.op = FilterOp.valueOf(opStr.toUpperCase().substring(1));
    } catch (IllegalArgumentException e) {
      throw new DocumentAPIRequestException(
          String.format(
              "Invalid operator: %s, valid operators are: %s", opStr, FilterOp.allRawValues()));
    }
    this.listValue = listValue;
  }

  @Override
  public List<Object> getValue() {
    return listValue;
  }

  @Override
  public FilterOp getFilterOp() {
    return op;
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public String getPathString() {
    return String.join(".", path);
  }

  @Override
  public String getFullFieldPath() {
    if (getPathString().isEmpty()) return field;
    return getPathString() + "." + field;
  }

  @Override
  public List<String> getPath() {
    return path;
  }

  @Override
  public String toString() {
    return String.format("ListFilterCondition{%s, %s, %s, %s}", path, field, op, listValue);
  }
}
