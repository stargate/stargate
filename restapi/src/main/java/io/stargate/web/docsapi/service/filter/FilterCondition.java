package io.stargate.web.docsapi.service.filter;

import java.util.List;

public interface FilterCondition {
  FilterOp getFilterOp();

  Object getValue();

  String getField();

  String getFullFieldPath();

  List<String> getPath();
}
