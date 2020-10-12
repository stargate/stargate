package io.stargate.web.docsapi.service.filter;

import io.stargate.web.docsapi.exception.SchemalessRequestException;
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
      throw new SchemalessRequestException(
          String.format(
              "Invalid operator: %s, valid operators are: %s", opStr, FilterOp.allRawValues()));
    }
    this.listValue = listValue;
  }

  public List<Object> getValue() {
    return listValue;
  }

  public FilterOp getFilterOp() {
    return op;
  }

  public String getField() {
    return field;
  }

  public String getPathString() {
    return String.join(".", path);
  }

  public String getFullFieldPath() {
    return getPathString() + "." + field;
  }

  public List<String> getPath() {
    return path;
  }

  public String toString() {
    return String.format("ListFilterCondition{%s, %s, %s, %s}", path, field, op, listValue);
  }
}
