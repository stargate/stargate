package io.stargate.sgv2.restapi.service.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.util.Collections;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

// Note: copy from SGv1 TableResponse
@RegisterForReflection
@Schema(name = "TableResponse", description = "A description of a Table")
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Sgv2Table(
    @Schema(description = "The name of the table.", nullable = false, example = "cycling_events")
        String name,
    @Schema(
            description = "Name of the keyspace the table belongs.",
            nullable = false,
            example = "cycling")
        String keyspace,
    @Schema(description = "Definition of columns within the table.", nullable = false)
        List<Sgv2ColumnDefinition> columnDefinitions,
    @Schema(
            description =
                "The definition of the partition and clustering keys that make up the primary key.")
        PrimaryKey primaryKey,
    @Schema(description = "Table options that are applied to the table.")
        TableOptions tableOptions) {
  public Sgv2Table(
      final String name,
      final String keyspace,
      final List<Sgv2ColumnDefinition> columnDefinitions,
      final PrimaryKey primaryKey,
      final TableOptions tableOptions) {
    this.name = name;
    this.keyspace = keyspace;
    this.columnDefinitions =
        (columnDefinitions == null) ? Collections.emptyList() : columnDefinitions;
    this.primaryKey = (primaryKey == null) ? new PrimaryKey() : primaryKey;
    this.tableOptions = tableOptions;
  }

  /*
  /////////////////////////////////////////////////////////////////////////
  // Nested classes used within
  /////////////////////////////////////////////////////////////////////////
   */

  @Schema(
      description =
          "Defines a column list for the primary key. Can be either a single column, compound primary key, or composite partition key. Provide multiple columns for the partition key to define a composite partition key.")
  public record PrimaryKey(
      @Schema(
              required = true,
              description = "Name(s) of the column(s) that constitute the partition key.")
          List<String> partitionKey,
      @Schema(description = "Name(s) of the column(s) that constitute the clustering key.")
          List<String> clusteringKey) {
    public PrimaryKey(List<String> partitionKey, List<String> clusteringKey) {
      this.partitionKey = (partitionKey == null) ? Collections.emptyList() : partitionKey;
      this.clusteringKey = (clusteringKey == null) ? Collections.emptyList() : clusteringKey;
    }

    public PrimaryKey(List<String> partitionKey) {
      this(partitionKey, null);
    }

    public PrimaryKey() {
      this(null, null);
    }

    public boolean hasPartitionKey(String key) {
      return partitionKey.contains(key);
    }

    public boolean hasClusteringKey(String key) {
      return clusteringKey.contains(key);
    }
  }

  // copy of SGv1 TableOptions
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record TableOptions(
      @Schema(
              description =
                  "Defines the Time To Live (TTL), which determines the time period (in seconds) to expire data. If the value is >0, TTL is enabled for the entire table and an expiration timestamp is added to each column. The maximum value is 630720000 (20 years). A new TTL timestamp is calculated each time the data is updated and the row is removed after the data expires.")
          Integer defaultTimeToLive,
      @Schema(
              description =
                  "Order rows storage to make use of the on-disk sorting of columns. Specifying order can make query results more efficient. Defaults to ascending if not provided.")
          List<ClusteringExpression> clusteringExpression) {
    public TableOptions() {
      this(null, null);
    }
  }

  // copied from SGv1 ClusteringExpression
  public record ClusteringExpression(
      @Schema(required = true, description = "The name of the column to order by") String column,
      @Schema(
              required = true,
              description = "The clustering order",
              enumeration = {"ASC", "DESC"})
          String order) {
    public static final String VALUE_ASC = "ASC";
    public static final String VALUE_DESC = "DESC";

    public boolean hasOrderAsc() {
      return VALUE_ASC.equalsIgnoreCase(order);
    }

    public boolean hasOrderDesc() {
      return VALUE_DESC.equalsIgnoreCase(order);
    }
  }
}
