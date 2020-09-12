package io.stargate.web.models;

import java.util.List;

public class Query {
  List<String> columnNames;
  List<Filter> filters;
  ClusteringExpression orderBy;
  Integer pageSize;
  String pageState;

  public List<String> getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public void setFilters(List<Filter> filters) {
    this.filters = filters;
  }

  public ClusteringExpression getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(ClusteringExpression orderBy) {
    this.orderBy = orderBy;
  }

  public Integer getPageSize() {
    return pageSize;
  }

  public void setPageSize(Integer pageSize) {
    this.pageSize = pageSize;
  }

  public String getPageState() {
    return pageState;
  }

  public void setPageState(String pageState) {
    this.pageState = pageState;
  }
}