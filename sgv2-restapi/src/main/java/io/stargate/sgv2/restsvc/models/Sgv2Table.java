package io.stargate.sgv2.restsvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// Note: copy from SGv1 TableResponse
@ApiModel(value = "TableResponse", description = "A description of a Table")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sgv2Table {
  private final String name;
  private final String keyspace;
  private final List<Sgv2ColumnDefinition> columnDefinitions;
  private final PrimaryKey primaryKey;
  private final TableOptions tableOptions;

  @JsonCreator
  public Sgv2Table(
      @JsonProperty("name") final String name,
      @JsonProperty("keyspace") final String keyspace,
      @JsonProperty("columnDefinitions") final List<Sgv2ColumnDefinition> columnDefinitions,
      @JsonProperty("primaryKey") final PrimaryKey primaryKey,
      @JsonProperty("tableOptions") final TableOptions tableOptions) {
    this.name = name;
    this.keyspace = keyspace;
    this.columnDefinitions =
        (columnDefinitions == null) ? Collections.emptyList() : columnDefinitions;
    this.primaryKey = (primaryKey == null) ? new PrimaryKey() : primaryKey;
    this.tableOptions = tableOptions;
  }

  @ApiModelProperty(value = "The name of the table.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(value = "Name of the keyspace the table belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @ApiModelProperty(value = "Definition of columns within the table.")
  public List<Sgv2ColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  @ApiModelProperty(
      value = "The definition of the partition and clustering keys that make up the primary key.")
  public PrimaryKey getPrimaryKey() {
    return primaryKey;
  }

  @ApiModelProperty(value = "Table options that are applied to the table.")
  public TableOptions getTableOptions() {
    return tableOptions;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Nested classes used within
  /////////////////////////////////////////////////////////////////////////
   */

  @ApiModel(
      value = "PrimaryKey",
      description =
          "Defines a column list for the primary key. Can be either a single column, compound primary key, or composite partition key. Provide multiple columns for the partition key to define a composite partition key.")
  public static class PrimaryKey {
    private List<String> partitionKey;
    private List<String> clusteringKey;

    public PrimaryKey(final List<String> partitionKey, final List<String> clusteringKey) {
      this.partitionKey = partitionKey;
      this.clusteringKey = clusteringKey;
    }

    public PrimaryKey() {
      this(new ArrayList<>(), new ArrayList<>());
    }

    public void addPartitionKey(String key) {
      partitionKey.add(key);
    }

    public void addClusteringKey(String key) {
      clusteringKey.add(key);
    }

    @ApiModelProperty(
        required = true,
        value = "Name of the column(s) that constitute the partition key.")
    public List<String> getPartitionKey() {
      return partitionKey;
    }

    @ApiModelProperty(value = "Name of the column or columns that constitute the clustering key.")
    public List<String> getClusteringKey() {
      return clusteringKey;
    }

    public void setPartitionKey(List<String> partitionKey) {
      this.partitionKey = partitionKey;
    }

    public void setClusteringKey(List<String> clusteringKey) {
      this.clusteringKey = clusteringKey;
    }

    public boolean hasPartitionKey(String key) {
      return (partitionKey != null) && partitionKey.contains(key);
    }

    public boolean hasClusteringKey(String key) {
      return (clusteringKey != null) && clusteringKey.contains(key);
    }
  }

  // copy of SGv1 TableOptions
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @ApiModel(value = "TableOptions")
  public static class TableOptions {
    private Integer defaultTimeToLive;
    private List<ClusteringExpression> clusteringExpression;

    @JsonCreator
    public TableOptions(
        @JsonProperty("defaultTimeToLive") final Integer defaultTimeToLive,
        @JsonProperty("clusteringExpression")
            final List<ClusteringExpression> clusteringExpression) {
      this.defaultTimeToLive = defaultTimeToLive;
      this.clusteringExpression = clusteringExpression;
    }

    public TableOptions() {
      this(null, null);
    }

    @ApiModelProperty(
        value =
            "Defines the Time To Live (TTL), which determines the time period (in seconds) to expire data. If the value is >0, TTL is enabled for the entire table and an expiration timestamp is added to each column. The maximum value is 630720000 (20 years). A new TTL timestamp is calculated each time the data is updated and the row is removed after the data expires.")
    public Integer getDefaultTimeToLive() {
      return defaultTimeToLive;
    }

    @ApiModelProperty(
        value =
            "Order rows storage to make use of the on-disk sorting of columns. Specifying order can make query results more efficient. Defaults to ascending if not provided.")
    public List<ClusteringExpression> getClusteringExpression() {
      return clusteringExpression;
    }

    public void setDefaultTimeToLive(int defaultTimeToLive) {
      this.defaultTimeToLive = defaultTimeToLive;
    }

    public void setClusteringExpression(List<ClusteringExpression> clusteringExpression) {
      this.clusteringExpression = clusteringExpression;
    }
  }

  // copied from SGv1 ClusteringExpression
  @ApiModel(value = "ClusteringExpression")
  public static class ClusteringExpression {
    public static final String VALUE_ASC = "ASC";
    public static final String VALUE_DESC = "DESC";

    private final String column;
    private final String order;

    @JsonCreator
    public ClusteringExpression(
        @JsonProperty("name") String column, @JsonProperty("order") String order) {
      this.column = column;
      this.order = order;
    }

    @ApiModelProperty(required = true, value = "The name of the column to order by")
    public String getColumn() {
      return column;
    }

    @ApiModelProperty(required = true, value = "The clustering order", allowableValues = "ASC,DESC")
    public String getOrder() {
      return order;
    }

    public boolean hasOrderAsc() {
      return VALUE_ASC.equalsIgnoreCase(order);
    }

    public boolean hasOrderDesc() {
      return VALUE_DESC.equalsIgnoreCase(order);
    }
  }
}