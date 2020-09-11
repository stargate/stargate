package io.stargate.web.models;

import java.util.List;

public class RowAdd {
  List<ColumnModel> columns;

  public List<ColumnModel> getColumns() {
    return columns;
  }

  public void setColumns(List<ColumnModel> columns) {
    this.columns = columns;
  }

  public RowAdd() {
  }

  public RowAdd(List<ColumnModel> columns) {
    this.columns = columns;
  }
}