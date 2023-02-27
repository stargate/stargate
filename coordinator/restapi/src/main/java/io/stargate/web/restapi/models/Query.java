/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.web.restapi.models;

import io.swagger.annotations.ApiModelProperty;
import java.util.List;

public class Query {

  private List<String> columnNames;
  private List<Filter> filters;
  private ClusteringExpression orderBy;
  private Integer pageSize;
  private String pageState;

  @ApiModelProperty(
      value =
          "A list of column names to return in the result set. An empty array returns all columns.")
  public List<String> getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  @ApiModelProperty(
      required = true,
      value =
          "An array of filters to return results for, separated by `AND`. For example, `a > 1 AND b != 1`.")
  public List<Filter> getFilters() {
    return filters;
  }

  public void setFilters(List<Filter> filters) {
    this.filters = filters;
  }

  @ApiModelProperty(value = "The clustering order for rows returned.")
  public ClusteringExpression getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(ClusteringExpression orderBy) {
    this.orderBy = orderBy;
  }

  @ApiModelProperty(value = "The size of the page to return in the result set.")
  public Integer getPageSize() {
    return pageSize;
  }

  public void setPageSize(Integer pageSize) {
    this.pageSize = pageSize;
  }

  @ApiModelProperty(
      value = "A string returned from previous query requests representing the paging state.")
  public String getPageState() {
    return pageState;
  }

  public void setPageState(String pageState) {
    this.pageState = pageState;
  }
}
